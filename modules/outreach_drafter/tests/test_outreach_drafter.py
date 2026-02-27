"""Tests for Outreach Drafter — Module 5.

All tests are pure unit tests (no live network, Odoo, or Claude calls).
drafter.py and main.py are tested via mocking.
"""

from datetime import date
from unittest.mock import MagicMock, patch, call

import pytest

from modules.outreach_drafter.drafter import (
    CONTEXT_FIELDS,
    STAGE_TEMPLATE_MAP,
    _resolve_many2one,
    assemble_lead_context,
    build_prompt,
    draft_outreach,
    select_template,
)


# ===========================================================================
# Fixtures
# ===========================================================================

def make_lead(**kwargs):
    defaults = {
        "id": 1,
        "name": "Test Lead",
        "partner_name": "Acme Corp",
        "contact_name": "Jane Doe",
        "email_from": "jane@acme.com",
        "phone": "555-1234",
        "city": "Metropolis",
        "street": "123 Main St",
        "state_id": [42, "New York"],
        "stage_id": [3, "Qualified"],
        "x_bd_stream": "stream_c",
        "x_business_type": "manufacturer",
        "x_current_supplier": "Global Supplies Inc",
        "x_import_source_country": "Country X",
        "x_estimated_spaces": False,
        "x_current_operator": False,
        "x_property_type": False,
        "x_company_size": "medium",
        "x_lead_score": 75,
        "x_decision_maker_title": "Procurement Manager",
        "x_outreach_draft": False,
        "description": "Manufactures consumer products.",
        "x_enrichment_status": "complete",
    }
    defaults.update(kwargs)
    return defaults


def make_templates():
    return {
        "stream_c": {
            "initial_contact": {
                "channel": "email",
                "tone": "professional, warm, relationship-focused",
                "max_length": 150,
                "structure": [
                    "Specific compliment about their business",
                    "Brief intro",
                    "Value proposition",
                    "Soft ask",
                ],
                "context_to_include": [
                    "x_current_supplier (mention switching value)",
                    "x_business_type (tailor language)",
                ],
                "example_subject_lines": [
                    "[Something specific] + product question",
                    "Saw your [product] — quick thought",
                ],
            },
            "sample_followup": {
                "channel": "email",
                "tone": "friendly, brief, curious",
                "max_length": 80,
                "structure": [
                    "Check if samples arrived",
                    "Ask about quality",
                    "Offer next step",
                ],
            },
        },
        "stream_a": {
            "initial_contact": {
                "channel": "email",
                "tone": "professional, local, solution-oriented",
                "max_length": 120,
                "structure": [
                    "Reference the property",
                    "Brief company intro",
                    "Value prop",
                    "Ask for meeting",
                ],
            },
        },
    }


# ===========================================================================
# 1. _resolve_many2one
# ===========================================================================

class TestResolveMany2one:
    def test_tuple_many2one(self):
        assert _resolve_many2one([42, "New York"]) == "New York"

    def test_tuple_many2one_actual_tuple(self):
        assert _resolve_many2one((42, "New York")) == "New York"

    def test_plain_string(self):
        assert _resolve_many2one("Metropolis") == "Metropolis"

    def test_plain_int(self):
        assert _resolve_many2one(75) == 75

    def test_false_value(self):
        assert _resolve_many2one(False) is False

    def test_empty_string(self):
        assert _resolve_many2one("") == ""

    def test_none(self):
        assert _resolve_many2one(None) is None

    def test_single_element_list_not_many2one(self):
        assert _resolve_many2one([42]) == [42]

    def test_three_element_list_not_many2one(self):
        assert _resolve_many2one([1, 2, 3]) == [1, 2, 3]


# ===========================================================================
# 2. assemble_lead_context
# ===========================================================================

class TestAssembleLeadContext:
    def test_full_lead(self):
        lead = make_lead()
        ctx = assemble_lead_context(lead)
        assert "Company: Acme Corp" in ctx
        assert "Contact Name: Jane Doe" in ctx
        assert "Email: jane@acme.com" in ctx
        assert "City: Metropolis" in ctx
        assert "State: New York" in ctx  # many2one resolved
        assert "Current Supplier: Global Supplies Inc" in ctx
        assert "Lead Score: 75" in ctx
        assert "Decision Maker Title: Procurement Manager" in ctx
        assert "Company Size: medium" in ctx
        assert "BD Stream: stream_c" in ctx
        assert "Business Type: manufacturer" in ctx
        assert "Import Source Country: Country X" in ctx

    def test_many2one_state_resolved(self):
        lead = make_lead(state_id=[42, "New York"])
        ctx = assemble_lead_context(lead)
        assert "State: New York" in ctx
        assert "42" not in ctx

    def test_many2one_stage_not_in_context(self):
        """stage_id is not in CONTEXT_FIELDS so it should not appear."""
        lead = make_lead()
        ctx = assemble_lead_context(lead)
        assert "stage_id" not in ctx.lower()

    def test_empty_fields_omitted(self):
        lead = make_lead(
            x_current_supplier=False,
            x_estimated_spaces=False,
            x_current_operator="",
            x_property_type=False,
        )
        ctx = assemble_lead_context(lead)
        assert "Current Supplier" not in ctx
        assert "Estimated Spaces" not in ctx
        assert "Current Operator" not in ctx
        assert "Property Type" not in ctx

    def test_sparse_lead(self):
        lead = {
            "id": 99,
            "partner_name": "Minimal Co",
            "x_bd_stream": "stream_a",
        }
        ctx = assemble_lead_context(lead)
        assert "Company: Minimal Co" in ctx
        assert "BD Stream: stream_a" in ctx
        # Only two lines
        lines = [l for l in ctx.strip().split("\n") if l.strip()]
        assert len(lines) == 2

    def test_zero_lead_score_included(self):
        """Zero is a valid numeric value, should be included."""
        lead = make_lead(x_lead_score=0)
        ctx = assemble_lead_context(lead)
        assert "Lead Score: 0" in ctx

    def test_empty_description_omitted(self):
        lead = make_lead(description="")
        ctx = assemble_lead_context(lead)
        assert "Background" not in ctx

    def test_false_description_omitted(self):
        lead = make_lead(description=False)
        ctx = assemble_lead_context(lead)
        assert "Background" not in ctx

    def test_description_included_when_present(self):
        lead = make_lead(description="Produces widgets.")
        ctx = assemble_lead_context(lead)
        assert "Background: Produces widgets." in ctx

    def test_completely_empty_lead(self):
        ctx = assemble_lead_context({})
        assert ctx == ""


# ===========================================================================
# 3. select_template
# ===========================================================================

class TestSelectTemplate:
    def test_qualified_maps_to_initial_contact(self):
        templates = make_templates()
        result = select_template("stream_c", "Qualified", templates)
        assert result is not None
        key, tmpl = result
        assert key == "initial_contact"
        assert tmpl["tone"] == "professional, warm, relationship-focused"

    def test_samples_sent_maps_to_sample_followup(self):
        templates = make_templates()
        result = select_template("stream_c", "Samples Sent", templates)
        assert result is not None
        key, tmpl = result
        assert key == "sample_followup"
        assert tmpl["tone"] == "friendly, brief, curious"

    def test_unknown_stage_returns_none(self):
        templates = make_templates()
        result = select_template("stream_c", "Negotiating", templates)
        assert result is None

    def test_unknown_stream_returns_none(self):
        templates = make_templates()
        result = select_template("nonexistent_stream", "Qualified", templates)
        assert result is None

    def test_missing_template_key_returns_none(self):
        """Stream exists but doesn't have the needed template."""
        templates = make_templates()
        result = select_template("stream_a", "Samples Sent", templates)
        assert result is None

    def test_empty_templates_returns_none(self):
        result = select_template("stream_c", "Qualified", {})
        assert result is None

    def test_research_stage_returns_none(self):
        templates = make_templates()
        result = select_template("stream_c", "Research", templates)
        assert result is None

    def test_stream_a_qualified(self):
        templates = make_templates()
        result = select_template("stream_a", "Qualified", templates)
        assert result is not None
        key, tmpl = result
        assert key == "initial_contact"
        assert tmpl["max_length"] == 120


# ===========================================================================
# 4. build_prompt
# ===========================================================================

class TestBuildPrompt:
    def test_includes_tone(self):
        template = make_templates()["stream_c"]["initial_contact"]
        prompt = build_prompt("Company: Acme Corp", template)
        assert "professional, warm, relationship-focused" in prompt

    def test_includes_max_length(self):
        template = make_templates()["stream_c"]["initial_contact"]
        prompt = build_prompt("Company: Acme Corp", template)
        assert "150 words" in prompt

    def test_includes_structure_as_numbered_list(self):
        template = make_templates()["stream_c"]["initial_contact"]
        prompt = build_prompt("Company: Acme Corp", template)
        assert "1. Specific compliment about their business" in prompt
        assert "2. Brief intro" in prompt
        assert "3. Value proposition" in prompt
        assert "4. Soft ask" in prompt

    def test_includes_context_to_include(self):
        template = make_templates()["stream_c"]["initial_contact"]
        prompt = build_prompt("Company: Acme Corp", template)
        assert "x_current_supplier (mention switching value)" in prompt
        assert "x_business_type (tailor language)" in prompt

    def test_includes_example_subject_lines(self):
        template = make_templates()["stream_c"]["initial_contact"]
        prompt = build_prompt("Company: Acme Corp", template)
        assert "[Something specific] + product question" in prompt
        assert "Saw your [product] — quick thought" in prompt

    def test_includes_lead_context_block(self):
        lead_context = "Company: Acme Corp\nCity: Metropolis"
        template = make_templates()["stream_c"]["initial_contact"]
        prompt = build_prompt(lead_context, template)
        assert "--- Lead Information ---" in prompt
        assert "Company: Acme Corp" in prompt
        assert "City: Metropolis" in prompt
        assert "--- End Lead Information ---" in prompt

    def test_handles_optional_fields_missing(self):
        """Template without context_to_include or example_subject_lines."""
        template = make_templates()["stream_c"]["sample_followup"]
        prompt = build_prompt("Company: Test", template)
        assert "Context to incorporate" not in prompt
        assert "Example subject line" not in prompt
        # But structure and tone should still be there
        assert "friendly, brief, curious" in prompt
        assert "1. Check if samples arrived" in prompt

    def test_empty_structure(self):
        template = {"tone": "friendly", "max_length": 100, "structure": []}
        prompt = build_prompt("Company: Test", template)
        assert "Email structure:" not in prompt

    def test_no_tone(self):
        template = {"max_length": 100, "structure": ["Intro"]}
        prompt = build_prompt("Company: Test", template)
        assert "Tone:" not in prompt


# ===========================================================================
# 5. draft_outreach
# ===========================================================================

class TestDraftOutreach:
    def test_calls_llm_and_appends_footer(self):
        lead = make_lead()
        template = make_templates()["stream_c"]["initial_contact"]
        mock_llm = MagicMock()
        mock_llm.complete.return_value = "Dear Jane,\n\nGreat email body here."

        result = draft_outreach(
            lead=lead,
            template_key="initial_contact",
            template=template,
            llm=mock_llm,
            system_prompt="You are drafting emails.",
            llm_model="claude-sonnet-4-6",
            max_tokens=512,
        )

        assert "Dear Jane," in result
        assert "Great email body here." in result
        assert "\n---\n[Drafted by AI — template: stream_c/initial_contact]" in result
        mock_llm.complete.assert_called_once()

    def test_llm_called_with_correct_params(self):
        lead = make_lead()
        template = make_templates()["stream_c"]["initial_contact"]
        mock_llm = MagicMock()
        mock_llm.complete.return_value = "Draft text"

        draft_outreach(
            lead=lead,
            template_key="initial_contact",
            template=template,
            llm=mock_llm,
            system_prompt="System prompt here",
            llm_model="claude-sonnet-4-6",
            max_tokens=512,
        )

        call_kwargs = mock_llm.complete.call_args
        assert call_kwargs.kwargs["system"] == "System prompt here"
        assert call_kwargs.kwargs["model"] == "claude-sonnet-4-6"
        assert call_kwargs.kwargs["max_tokens"] == 512
        # Prompt should include lead context
        assert "Acme Corp" in call_kwargs.kwargs["prompt"]

    def test_footer_uses_stream_from_lead(self):
        lead = make_lead(x_bd_stream="stream_a")
        template = make_templates()["stream_a"]["initial_contact"]
        mock_llm = MagicMock()
        mock_llm.complete.return_value = "Draft"

        result = draft_outreach(
            lead=lead,
            template_key="initial_contact",
            template=template,
            llm=mock_llm,
            system_prompt="",
            llm_model="claude-sonnet-4-6",
            max_tokens=512,
        )
        assert "template: stream_a/initial_contact" in result

    def test_missing_stream_uses_unknown(self):
        lead = make_lead(x_bd_stream=False)
        lead.pop("x_bd_stream")
        template = {"tone": "friendly", "max_length": 100, "structure": ["Intro"]}
        mock_llm = MagicMock()
        mock_llm.complete.return_value = "Draft"

        result = draft_outreach(
            lead=lead,
            template_key="initial_contact",
            template=template,
            llm=mock_llm,
            system_prompt="",
            llm_model="claude-sonnet-4-6",
            max_tokens=512,
        )
        assert "template: unknown/initial_contact" in result

    def test_llm_exception_propagates(self):
        lead = make_lead()
        template = make_templates()["stream_c"]["initial_contact"]
        mock_llm = MagicMock()
        mock_llm.complete.side_effect = RuntimeError("API error")

        with pytest.raises(RuntimeError, match="API error"):
            draft_outreach(
                lead=lead,
                template_key="initial_contact",
                template=template,
                llm=mock_llm,
                system_prompt="",
                llm_model="claude-sonnet-4-6",
                max_tokens=512,
            )


# ===========================================================================
# 6. main.py — fetch_leads_needing_outreach
# ===========================================================================

class TestFetchLeadsNeedingOutreach:
    def test_domain_construction_no_stream(self):
        from modules.outreach_drafter.main import fetch_leads_needing_outreach

        mock_odoo = MagicMock()
        mock_odoo.search_leads.return_value = []

        fetch_leads_needing_outreach(mock_odoo, stream_filter=None, limit=None)

        domain = mock_odoo.search_leads.call_args[0][0]
        # Should have 4 conditions: x_outreach_draft empty, stage in set, not terminal, active
        assert ["x_outreach_draft", "in", [False, ""]] in domain
        assert ["stage_id.name", "not in", list({"Won", "Lost"})] in domain
        assert ["active", "in", [True, False]] in domain
        # Should NOT have stream filter
        stream_conditions = [d for d in domain if d[0] == "x_bd_stream"]
        assert len(stream_conditions) == 0

    def test_domain_construction_with_stream(self):
        from modules.outreach_drafter.main import fetch_leads_needing_outreach

        mock_odoo = MagicMock()
        mock_odoo.search_leads.return_value = []

        fetch_leads_needing_outreach(mock_odoo, stream_filter="stream_b", limit=5)

        domain = mock_odoo.search_leads.call_args[0][0]
        assert ["x_bd_stream", "=", "stream_b"] in domain
        # limit passed through
        assert mock_odoo.search_leads.call_args[1]["limit"] == 5

    def test_stage_filter_includes_outreach_stages(self):
        from modules.outreach_drafter.main import fetch_leads_needing_outreach, OUTREACH_STAGES

        mock_odoo = MagicMock()
        mock_odoo.search_leads.return_value = []

        fetch_leads_needing_outreach(mock_odoo, stream_filter=None, limit=None)

        domain = mock_odoo.search_leads.call_args[0][0]
        stage_in = [d for d in domain if d[0] == "stage_id.name" and d[1] == "in"]
        assert len(stage_in) == 1
        assert set(stage_in[0][2]) == OUTREACH_STAGES

    def test_returns_leads(self):
        from modules.outreach_drafter.main import fetch_leads_needing_outreach

        mock_odoo = MagicMock()
        mock_odoo.search_leads.return_value = [make_lead(), make_lead(id=2)]

        result = fetch_leads_needing_outreach(mock_odoo, None, None)
        assert len(result) == 2


# ===========================================================================
# 7. main.py — run() end-to-end
# ===========================================================================

class TestRun:
    def _setup_mocks(self, leads=None):
        mock_odoo = MagicMock()
        mock_odoo.search_leads.return_value = leads or []
        mock_odoo.update_lead.return_value = True
        mock_odoo.create_activity.return_value = 1
        return mock_odoo

    def _setup_llm(self, response="Dear Jane,\n\nGreat draft."):
        mock_llm = MagicMock()
        mock_llm.complete.return_value = response
        mock_llm.get_cost_summary.return_value = {
            "calls": 1, "input_tokens": 500, "output_tokens": 200, "cost_usd": 0.0045,
        }
        return mock_llm

    def _config(self):
        return {
            "templates": make_templates(),
            "llm": {
                "model": "claude-sonnet-4-6",
                "max_tokens": 512,
                "system_prompt": "You are drafting outreach emails.",
            },
        }

    def _run_with_mocks(self, leads, dry_run=False, stream_filter=None, limit=None, llm_response="Draft text"):
        from modules.outreach_drafter.main import run

        mock_odoo = self._setup_mocks(leads)
        mock_llm = self._setup_llm(llm_response)

        with patch("modules.outreach_drafter.main.OdooClient.from_env", return_value=mock_odoo), \
             patch("modules.outreach_drafter.main.LLMClient.from_env", return_value=mock_llm), \
             patch("modules.outreach_drafter.main.load_config", return_value=self._config()):
            result = run(dry_run=dry_run, stream_filter=stream_filter, limit=limit)

        return result, mock_odoo, mock_llm

    def test_no_eligible_leads(self):
        result, mock_odoo, mock_llm = self._run_with_mocks(leads=[])
        assert result == {"drafted": 0, "skipped": 0, "errors": 0, "total": 0}
        mock_llm.complete.assert_not_called()
        mock_odoo.update_lead.assert_not_called()

    def test_single_lead_drafted(self):
        lead = make_lead()
        result, mock_odoo, mock_llm = self._run_with_mocks(leads=[lead])

        assert result["drafted"] == 1
        assert result["total"] == 1
        assert result["errors"] == 0
        assert result["skipped"] == 0

        # Verify LLM was called
        mock_llm.complete.assert_called_once()

        # Verify Odoo updated
        mock_odoo.update_lead.assert_called_once()
        update_args = mock_odoo.update_lead.call_args[0]
        assert update_args[0] == 1  # lead id
        assert "x_outreach_draft" in update_args[1]
        assert "template: stream_c/initial_contact" in update_args[1]["x_outreach_draft"]

    def test_activity_created(self):
        lead = make_lead()
        result, mock_odoo, mock_llm = self._run_with_mocks(leads=[lead])

        mock_odoo.create_activity.assert_called_once()
        activity_kwargs = mock_odoo.create_activity.call_args
        assert activity_kwargs.kwargs["lead_id"] == 1 or activity_kwargs[1].get("lead_id") == 1 or activity_kwargs[0][0] == 1

    def test_activity_summary_and_deadline(self):
        lead = make_lead()
        result, mock_odoo, mock_llm = self._run_with_mocks(leads=[lead])

        call_kwargs = mock_odoo.create_activity.call_args
        # Depending on how it's called (positional or keyword)
        if call_kwargs.kwargs:
            assert call_kwargs.kwargs["summary"] == "Review and send outreach draft"
            assert call_kwargs.kwargs["date_deadline"] == date.today()
        else:
            assert call_kwargs[0][1] == "Review and send outreach draft"

    def test_dry_run_calls_llm_but_not_odoo(self):
        lead = make_lead()
        result, mock_odoo, mock_llm = self._run_with_mocks(leads=[lead], dry_run=True)

        assert result["drafted"] == 1
        # LLM should be called (we want to preview the draft)
        mock_llm.complete.assert_called_once()
        # But Odoo should NOT be written to
        mock_odoo.update_lead.assert_not_called()
        mock_odoo.create_activity.assert_not_called()

    def test_multiple_leads_all_drafted(self):
        leads = [
            make_lead(id=1, partner_name="Acme Corp"),
            make_lead(id=2, partner_name="Beta Inc"),
            make_lead(id=3, partner_name="Gamma LLC"),
        ]
        result, mock_odoo, mock_llm = self._run_with_mocks(leads=leads)

        assert result["drafted"] == 3
        assert result["total"] == 3
        assert mock_llm.complete.call_count == 3
        assert mock_odoo.update_lead.call_count == 3
        assert mock_odoo.create_activity.call_count == 3

    def test_lead_with_no_template_skipped(self):
        """Lead with a stream that has no templates is skipped."""
        lead = make_lead(x_bd_stream="nonexistent_stream")
        result, mock_odoo, mock_llm = self._run_with_mocks(leads=[lead])

        assert result["skipped"] == 1
        assert result["drafted"] == 0
        mock_llm.complete.assert_not_called()

    def test_lead_with_wrong_stage_skipped(self):
        """Lead in a stage not in STAGE_TEMPLATE_MAP is skipped."""
        lead = make_lead(stage_id=[5, "Negotiating"])
        result, mock_odoo, mock_llm = self._run_with_mocks(leads=[lead])

        assert result["skipped"] == 1
        assert result["drafted"] == 0

    def test_llm_exception_mid_batch(self):
        """One LLM failure doesn't stop the entire batch."""
        leads = [
            make_lead(id=1, partner_name="Good Lead"),
            make_lead(id=2, partner_name="Bad Lead"),
            make_lead(id=3, partner_name="Another Good Lead"),
        ]
        from modules.outreach_drafter.main import run

        mock_odoo = self._setup_mocks(leads)
        mock_llm = self._setup_llm()
        # Second call raises
        mock_llm.complete.side_effect = [
            "Draft for lead 1",
            RuntimeError("LLM timeout"),
            "Draft for lead 3",
        ]

        with patch("modules.outreach_drafter.main.OdooClient.from_env", return_value=mock_odoo), \
             patch("modules.outreach_drafter.main.LLMClient.from_env", return_value=mock_llm), \
             patch("modules.outreach_drafter.main.load_config", return_value=self._config()):
            result = run()

        assert result["drafted"] == 2
        assert result["errors"] == 1
        assert result["total"] == 3

    def test_odoo_update_exception_counted_as_error(self):
        lead = make_lead()
        from modules.outreach_drafter.main import run

        mock_odoo = self._setup_mocks([lead])
        mock_odoo.update_lead.side_effect = Exception("Odoo write failed")
        mock_llm = self._setup_llm()

        with patch("modules.outreach_drafter.main.OdooClient.from_env", return_value=mock_odoo), \
             patch("modules.outreach_drafter.main.LLMClient.from_env", return_value=mock_llm), \
             patch("modules.outreach_drafter.main.load_config", return_value=self._config()):
            result = run()

        assert result["errors"] == 1
        assert result["drafted"] == 0

    def test_cost_summary_printed(self, capsys):
        lead = make_lead()
        result, mock_odoo, mock_llm = self._run_with_mocks(leads=[lead])

        captured = capsys.readouterr()
        assert "LLM calls: 1" in captured.out
        assert "$0.0045" in captured.out

    def test_summary_output_printed(self, capsys):
        lead = make_lead()
        result, mock_odoo, mock_llm = self._run_with_mocks(leads=[lead])

        captured = capsys.readouterr()
        assert "Outreach Drafter Summary" in captured.out
        assert "Drafted: 1" in captured.out

    def test_no_leads_prints_nothing(self, capsys):
        result, mock_odoo, mock_llm = self._run_with_mocks(leads=[])

        captured = capsys.readouterr()
        # No summary printed for empty run
        assert "Outreach Drafter Summary" not in captured.out

    def test_mixed_skipped_and_drafted(self):
        leads = [
            make_lead(id=1, x_bd_stream="stream_c"),  # has template
            make_lead(id=2, x_bd_stream="nonexistent"),  # no template
            make_lead(id=3, x_bd_stream="stream_c", stage_id=[5, "Negotiating"]),  # wrong stage
        ]
        result, mock_odoo, mock_llm = self._run_with_mocks(leads=leads)

        assert result["drafted"] == 1
        assert result["skipped"] == 2
        assert result["total"] == 3

    def test_samples_sent_stage_uses_sample_followup(self):
        lead = make_lead(stage_id=[7, "Samples Sent"])
        from modules.outreach_drafter.main import run

        mock_odoo = self._setup_mocks([lead])
        mock_llm = self._setup_llm("Sample followup draft")

        with patch("modules.outreach_drafter.main.OdooClient.from_env", return_value=mock_odoo), \
             patch("modules.outreach_drafter.main.LLMClient.from_env", return_value=mock_llm), \
             patch("modules.outreach_drafter.main.load_config", return_value=self._config()):
            result = run()

        assert result["drafted"] == 1
        update_args = mock_odoo.update_lead.call_args[0]
        assert "template: stream_c/sample_followup" in update_args[1]["x_outreach_draft"]

    def test_draft_stored_in_x_outreach_draft(self):
        lead = make_lead()
        result, mock_odoo, mock_llm = self._run_with_mocks(leads=[lead], llm_response="Hello Jane!")

        update_args = mock_odoo.update_lead.call_args[0]
        draft = update_args[1]["x_outreach_draft"]
        assert "Hello Jane!" in draft
        assert "[Drafted by AI" in draft

    def test_lead_with_partner_name_fallback(self):
        """If partner_name is missing, falls back to name."""
        lead = make_lead(partner_name=False, name="Fallback Name")
        result, mock_odoo, mock_llm = self._run_with_mocks(leads=[lead])
        assert result["drafted"] == 1

    def test_lead_with_id_fallback(self):
        """If both partner_name and name are empty, uses #id."""
        lead = make_lead(partner_name=False, name=False)
        result, mock_odoo, mock_llm = self._run_with_mocks(leads=[lead])
        assert result["drafted"] == 1

    def test_create_activity_exception_counted_as_error(self):
        """If create_activity raises, it's counted as an error."""
        lead = make_lead()
        from modules.outreach_drafter.main import run

        mock_odoo = self._setup_mocks([lead])
        mock_odoo.create_activity.side_effect = Exception("Activity creation failed")
        mock_llm = self._setup_llm()

        with patch("modules.outreach_drafter.main.OdooClient.from_env", return_value=mock_odoo), \
             patch("modules.outreach_drafter.main.LLMClient.from_env", return_value=mock_llm), \
             patch("modules.outreach_drafter.main.load_config", return_value=self._config()):
            result = run()

        assert result["errors"] == 1
        assert result["drafted"] == 0

    def test_stream_a_qualified_draft(self):
        lead = make_lead(
            x_bd_stream="stream_a",
            stage_id=[3, "Qualified"],
            x_estimated_spaces=50,
            x_current_operator="Competitor Co",
            x_property_type="commercial",
        )
        result, mock_odoo, mock_llm = self._run_with_mocks(leads=[lead])

        assert result["drafted"] == 1
        update_args = mock_odoo.update_lead.call_args[0]
        assert "stream_a/initial_contact" in update_args[1]["x_outreach_draft"]

    def test_llm_model_from_config(self):
        """Verifies the LLM model from config is passed through."""
        lead = make_lead()
        from modules.outreach_drafter.main import run

        mock_odoo = self._setup_mocks([lead])
        mock_llm = self._setup_llm()

        config = self._config()
        config["llm"]["model"] = "claude-sonnet-4-6"

        with patch("modules.outreach_drafter.main.OdooClient.from_env", return_value=mock_odoo), \
             patch("modules.outreach_drafter.main.LLMClient.from_env", return_value=mock_llm), \
             patch("modules.outreach_drafter.main.load_config", return_value=config):
            run()

        call_kwargs = mock_llm.complete.call_args.kwargs
        assert call_kwargs["model"] == "claude-sonnet-4-6"

    def test_system_prompt_from_config(self):
        lead = make_lead()
        from modules.outreach_drafter.main import run

        mock_odoo = self._setup_mocks([lead])
        mock_llm = self._setup_llm()

        config = self._config()
        config["llm"]["system_prompt"] = "Custom system prompt"

        with patch("modules.outreach_drafter.main.OdooClient.from_env", return_value=mock_odoo), \
             patch("modules.outreach_drafter.main.LLMClient.from_env", return_value=mock_llm), \
             patch("modules.outreach_drafter.main.load_config", return_value=config):
            run()

        call_kwargs = mock_llm.complete.call_args.kwargs
        assert call_kwargs["system"] == "Custom system prompt"


# ===========================================================================
# 8. main.py — main() argparse
# ===========================================================================

class TestMainArgparse:
    def test_dry_run_flag(self):
        from modules.outreach_drafter.main import run

        with patch("modules.outreach_drafter.main.OdooClient.from_env") as mock_odoo_cls, \
             patch("modules.outreach_drafter.main.LLMClient.from_env") as mock_llm_cls, \
             patch("modules.outreach_drafter.main.load_config", return_value={"templates": {}, "llm": {}}):
            mock_odoo_cls.return_value.search_leads.return_value = []
            mock_llm_cls.return_value.get_cost_summary.return_value = {
                "calls": 0, "input_tokens": 0, "output_tokens": 0, "cost_usd": 0.0,
            }
            result = run(dry_run=True, stream_filter="stream_c", limit=5)
            assert result["total"] == 0
