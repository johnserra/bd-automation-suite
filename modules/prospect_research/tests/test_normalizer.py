"""Unit tests for prospect_research/normalizer.py."""

import pytest

from modules.prospect_research.normalizer import (
    ProspectRecord,
    _make_lead_title,
    parse_google_address,
)


class TestMakeLeadTitle:
    def test_basic(self):
        assert _make_lead_title("Acme Foods", "stream_c") == "Acme Foods — Stream C"

    def test_underscores_replaced(self):
        assert _make_lead_title("Park Co", "stream_a") == "Park Co — Stream A"


class TestParseGoogleAddress:
    def test_full_us_address(self):
        result = parse_google_address("123 Main St, Syracuse, NY 13202, USA")
        assert result["street"] == "123 Main St"
        assert result["city"] == "Syracuse"
        assert result["state_code"] == "NY"
        assert result["zip"] == "13202"

    def test_address_without_zip(self):
        result = parse_google_address("456 Oak Ave, Buffalo, NY, USA")
        assert result["street"] == "456 Oak Ave"
        assert result["city"] == "Buffalo"
        assert result["state_code"] == "NY"
        assert result["zip"] is None

    def test_city_state_only(self):
        result = parse_google_address("Rochester, NY, USA")
        assert result["city"] == "Rochester"
        assert result["state_code"] == "NY"
        assert result["street"] is None

    def test_new_york_city(self):
        result = parse_google_address("456 Park Ave, New York, NY 10022, USA")
        assert result["street"] == "456 Park Ave"
        assert result["city"] == "New York"
        assert result["state_code"] == "NY"
        assert result["zip"] == "10022"

    def test_empty_string(self):
        result = parse_google_address("")
        assert result == {"street": None, "city": None, "state_code": None, "zip": None}

    def test_united_states_variant(self):
        result = parse_google_address("789 Elm St, Albany, NY 12201, United States")
        assert result["city"] == "Albany"
        assert result["state_code"] == "NY"

    def test_zip_plus_four(self):
        result = parse_google_address("1 Trade Ctr, New York, NY 10007-0001, USA")
        assert result["zip"] == "10007-0001"


class TestProspectRecord:
    def test_minimal_record(self):
        rec = ProspectRecord(partner_name="Test Corp")
        assert rec.partner_name == "Test Corp"
        assert rec.street is None
        assert rec.x_enrichment_status == "pending"

    def test_to_odoo_values_minimal(self):
        rec = ProspectRecord(partner_name="Test Corp", x_data_source="trade_data")
        values = rec.to_odoo_values(stream="stream_c", stage_id=5)
        assert values["partner_name"] == "Test Corp"
        assert values["stage_id"] == 5
        assert values["x_bd_stream"] == "stream_c"
        assert values["x_data_source"] == "trade_data"
        assert "street" not in values        # None fields omitted
        assert "city" not in values

    def test_to_odoo_values_full(self):
        rec = ProspectRecord(
            partner_name="Acme Bakery Inc",
            street="100 Industrial Dr",
            city="Syracuse",
            state_code="NY",
            zip="13202",
            phone="315-555-0100",
            website="https://acmebakery.com",
            x_data_source="trade_data",
            x_already_importing=True,
            x_import_source_country="CN",
        )
        values = rec.to_odoo_values(
            stream="stream_c",
            stage_id=3,
            state_id=42,
            country_id=233,
        )
        assert values["street"] == "100 Industrial Dr"
        assert values["city"] == "Syracuse"
        assert values["state_id"] == 42
        assert values["zip"] == "13202"
        assert values["country_id"] == 233
        assert values["phone"] == "315-555-0100"
        assert values["website"] == "https://acmebakery.com"
        assert values["x_already_importing"] is True
        assert values["x_import_source_country"] == "CN"

    def test_to_odoo_values_no_state_id(self):
        """state_id should be omitted when not resolved."""
        rec = ProspectRecord(partner_name="X Corp", state_code="NY")
        values = rec.to_odoo_values(stream="stream_c", stage_id=1, state_id=None)
        assert "state_id" not in values

    def test_false_already_importing_omitted(self):
        """None x_already_importing should not appear in values."""
        rec = ProspectRecord(partner_name="Y Corp", x_already_importing=None)
        values = rec.to_odoo_values(stream="stream_c", stage_id=1)
        assert "x_already_importing" not in values

    def test_false_already_importing_included(self):
        """False (bool) x_already_importing IS a valid value and should be included."""
        rec = ProspectRecord(partner_name="Z Corp", x_already_importing=False)
        values = rec.to_odoo_values(stream="stream_c", stage_id=1)
        assert "x_already_importing" in values
        assert values["x_already_importing"] is False

    def test_parking_fields(self):
        rec = ProspectRecord(
            partner_name="Downtown Garage LLC",
            x_property_type="garage",
            x_estimated_spaces=120,
        )
        values = rec.to_odoo_values(stream="stream_a", stage_id=2)
        assert values["x_property_type"] == "garage"
        assert values["x_estimated_spaces"] == 120
