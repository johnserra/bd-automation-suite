"""Unit tests for prospect_research adapters and deduplicator.

All tests mock external calls (HTTP, Google Maps API, OdooClient) so no
live credentials are required.
"""

from unittest.mock import MagicMock, patch

import pytest

from modules.prospect_research.adapters.trade_data import (
    TradeDataAdapter,
    _parse_city_state,
    _normalize_country,
)
from modules.prospect_research.adapters.google_maps import GoogleMapsAdapter
from modules.prospect_research.deduplicator import is_duplicate, split_new_and_duplicate
from modules.prospect_research.normalizer import ProspectRecord


# ---------------------------------------------------------------------------
# TradeData adapter tests
# ---------------------------------------------------------------------------

SAMPLE_IMPORTYETI_HTML = """
<html><body>
<div class="company-list-item">
  <a href="/company/acme-bakery" class="company-name">Acme Bakery Inc</a>
  <span class="location">Syracuse, NY</span>
  <span class="supplier">China (CN)</span>
</div>
<div class="company-list-item">
  <a href="/company/fresh-foods" class="company-name">Fresh Foods Co</a>
  <span class="location">Rochester, NY</span>
  <span class="supplier">Vietnam (VN)</span>
</div>
<div class="company-list-item">
  <a href="/company/domestic-pack" class="company-name">Domestic Packaging LLC</a>
  <span class="location">Albany, NY</span>
  <span class="supplier">United States (US)</span>
</div>
</body></html>
"""

EMPTY_HTML = "<html><body><p>No results</p></body></html>"


class TestTradeDataAdapter:
    def _adapter(self):
        return TradeDataAdapter()

    def test_fetch_parses_cards(self):
        adapter = self._adapter()
        adapter._get_cached = MagicMock(return_value=SAMPLE_IMPORTYETI_HTML)

        records = adapter.fetch(
            adapter_config={"hs_codes": ["3923.30"], "exclude_suppliers_from": []},
            stream="stream_c",
            profile={"geography": {}},
        )
        assert len(records) == 3
        names = [r.partner_name for r in records]
        assert "Acme Bakery Inc" in names
        assert "Fresh Foods Co" in names

    def test_fetch_excludes_domestic_suppliers(self):
        adapter = self._adapter()
        adapter._get_cached = MagicMock(return_value=SAMPLE_IMPORTYETI_HTML)

        records = adapter.fetch(
            adapter_config={
                "hs_codes": ["3923.30"],
                "exclude_suppliers_from": ["US"],
            },
            stream="stream_c",
            profile={"geography": {}},
        )
        names = [r.partner_name for r in records]
        assert "Domestic Packaging LLC" not in names
        assert len(records) == 2

    def test_fetch_sets_already_importing(self):
        adapter = self._adapter()
        adapter._get_cached = MagicMock(return_value=SAMPLE_IMPORTYETI_HTML)

        records = adapter.fetch(
            adapter_config={"hs_codes": ["3923.30"], "exclude_suppliers_from": []},
            stream="stream_c",
            profile={"geography": {}},
        )
        for rec in records:
            assert rec.x_already_importing is True

    def test_fetch_dedupes_across_hs_codes(self):
        """Same company appearing under two HS codes should only appear once."""
        adapter = self._adapter()
        # Both HS codes return same HTML (same companies)
        adapter._get_cached = MagicMock(return_value=SAMPLE_IMPORTYETI_HTML)

        records = adapter.fetch(
            adapter_config={
                "hs_codes": ["3923.30", "3923.50"],
                "exclude_suppliers_from": [],
            },
            stream="stream_c",
            profile={"geography": {}},
        )
        names = [r.partner_name for r in records]
        # Should not have duplicates
        assert len(names) == len(set(names))

    def test_empty_page_returns_empty_list(self):
        adapter = self._adapter()
        adapter._get_cached = MagicMock(return_value=EMPTY_HTML)

        records = adapter.fetch(
            adapter_config={"hs_codes": ["3923.30"], "exclude_suppliers_from": []},
            stream="stream_c",
            profile={"geography": {}},
        )
        assert records == []

    def test_fetch_failure_returns_empty(self):
        """If HTTP fetch fails, return [] without raising."""
        adapter = self._adapter()
        adapter._get_cached = MagicMock(return_value=None)

        records = adapter.fetch(
            adapter_config={"hs_codes": ["3923.30"], "exclude_suppliers_from": []},
            stream="stream_c",
            profile={"geography": {}},
        )
        assert records == []

    def test_no_hs_codes_returns_empty(self):
        adapter = self._adapter()
        records = adapter.fetch(
            adapter_config={"hs_codes": [], "exclude_suppliers_from": []},
            stream="stream_c",
            profile={},
        )
        assert records == []

    def test_record_has_data_source(self):
        adapter = self._adapter()
        adapter._get_cached = MagicMock(return_value=SAMPLE_IMPORTYETI_HTML)

        records = adapter.fetch(
            adapter_config={"hs_codes": ["3923.30"], "exclude_suppliers_from": []},
            stream="stream_c",
            profile={},
        )
        for rec in records:
            assert rec.x_data_source == "trade_data"

    def test_city_state_parsed(self):
        adapter = self._adapter()
        adapter._get_cached = MagicMock(return_value=SAMPLE_IMPORTYETI_HTML)

        records = adapter.fetch(
            adapter_config={"hs_codes": ["3923.30"], "exclude_suppliers_from": []},
            stream="stream_c",
            profile={},
        )
        acme = next(r for r in records if "Acme" in r.partner_name)
        assert acme.city == "Syracuse"
        assert acme.state_code == "NY"


class TestParseCityState:
    def test_city_state(self):
        assert _parse_city_state("Syracuse, NY") == ("Syracuse", "NY")

    def test_city_state_no_comma(self):
        assert _parse_city_state("Buffalo NY") == ("Buffalo", "NY")

    def test_none(self):
        assert _parse_city_state(None) == (None, None)

    def test_empty(self):
        assert _parse_city_state("") == (None, None)

    def test_multi_word_city(self):
        city, state = _parse_city_state("New York, NY")
        assert city == "New York"
        assert state == "NY"


class TestNormalizeCountry:
    def test_iso_in_parens(self):
        assert _normalize_country("China (CN)") == "CN"

    def test_iso_only(self):
        # No parens — returns raw text
        result = _normalize_country("China")
        assert result == "China"

    def test_none(self):
        assert _normalize_country(None) is None

    def test_multiple_countries(self):
        result = _normalize_country("China (CN), Vietnam (VN)")
        # Extracts first ISO code found
        assert result == "CN"


# ---------------------------------------------------------------------------
# Google Maps adapter tests
# ---------------------------------------------------------------------------

SAMPLE_PLACES_RESPONSE = {
    "results": [
        {
            "name": "Clinton Square Parking",
            "formatted_address": "1 Clinton Square, Syracuse, NY 13202, USA",
            "place_id": "ChIJ_abc123",
            "rating": 4.1,
            "types": ["parking", "point_of_interest"],
        },
        {
            "name": "Example Operator - Downtown",
            "formatted_address": "200 South Warren St, Syracuse, NY 13202, USA",
            "place_id": "ChIJ_xyz456",
            "rating": 3.8,
            "types": ["parking"],
        },
        {
            "name": "Canal Street Garage",
            "formatted_address": "300 Canal St, Syracuse, NY 13202, USA",
            "place_id": "ChIJ_def789",
            "rating": 4.5,
            "types": ["parking", "establishment"],
        },
    ]
}


class TestGoogleMapsAdapter:
    def _adapter(self):
        return GoogleMapsAdapter(api_key="FAKE_KEY_FOR_TESTS")

    def _make_mock_client(self, response=None):
        mock_client = MagicMock()
        mock_client.places.return_value = response or SAMPLE_PLACES_RESPONSE
        return mock_client

    def test_fetch_returns_records(self):
        adapter = self._adapter()
        adapter._client = self._make_mock_client()

        records = adapter.fetch(
            adapter_config={
                "search_queries": ["parking lot {city}"],
                "fetch_details": False,
            },
            stream="stream_a",
            profile={"geography": {"cities": ["Syracuse"]}, "exclude_operators": []},
        )
        assert len(records) == 3
        names = [r.partner_name for r in records]
        assert "Clinton Square Parking" in names

    def test_excludes_own_operator(self):
        adapter = self._adapter()
        adapter._client = self._make_mock_client()

        records = adapter.fetch(
            adapter_config={
                "search_queries": ["parking lot {city}"],
                "fetch_details": False,
            },
            stream="stream_a",
            profile={
                "geography": {"cities": ["Syracuse"]},
                "exclude_operators": ["Example Operator"],
            },
        )
        names = [r.partner_name for r in records]
        assert "Example Operator - Downtown" not in names
        assert len(records) == 2

    def test_dedupes_by_place_id(self):
        """Same place from two queries should appear only once."""
        adapter = self._adapter()
        adapter._client = self._make_mock_client()

        records = adapter.fetch(
            adapter_config={
                "search_queries": ["parking lot {city}", "parking garage {city}"],
                "fetch_details": False,
            },
            stream="stream_a",
            profile={"geography": {"cities": ["Syracuse"]}, "exclude_operators": []},
        )
        place_ids = [r.place_id for r in records]
        assert len(place_ids) == len(set(place_ids))

    def test_address_parsed(self):
        adapter = self._adapter()
        adapter._client = self._make_mock_client()

        records = adapter.fetch(
            adapter_config={"search_queries": ["parking {city}"], "fetch_details": False},
            stream="stream_a",
            profile={"geography": {"cities": ["Syracuse"]}, "exclude_operators": []},
        )
        rec = records[0]
        assert rec.city == "Syracuse"
        assert rec.state_code == "NY"
        assert rec.zip == "13202"

    def test_data_source_set(self):
        adapter = self._adapter()
        adapter._client = self._make_mock_client()

        records = adapter.fetch(
            adapter_config={"search_queries": ["parking {city}"], "fetch_details": False},
            stream="stream_a",
            profile={"geography": {"cities": ["Syracuse"]}, "exclude_operators": []},
        )
        for rec in records:
            assert rec.x_data_source == "google_maps"

    def test_rating_captured(self):
        adapter = self._adapter()
        adapter._client = self._make_mock_client()

        records = adapter.fetch(
            adapter_config={"search_queries": ["parking {city}"], "fetch_details": False},
            stream="stream_a",
            profile={"geography": {"cities": ["Syracuse"]}, "exclude_operators": []},
        )
        rec = next(r for r in records if "Clinton" in r.partner_name)
        assert rec.rating == 4.1

    def test_api_error_returns_empty(self):
        """API errors should be caught; fetch returns whatever succeeded."""
        adapter = self._adapter()
        mock_client = MagicMock()
        mock_client.places.side_effect = Exception("API quota exceeded")
        adapter._client = mock_client

        records = adapter.fetch(
            adapter_config={"search_queries": ["parking {city}"], "fetch_details": False},
            stream="stream_a",
            profile={"geography": {"cities": ["Syracuse"]}, "exclude_operators": []},
        )
        assert records == []

    def test_no_api_key_raises_environment_error(self):
        adapter = GoogleMapsAdapter(api_key="")
        with pytest.raises(EnvironmentError, match="GOOGLE_MAPS_API_KEY"):
            adapter._get_client()

    def test_multi_city_expansion(self):
        """Each city should produce a separate API call."""
        adapter = self._adapter()
        mock_client = self._make_mock_client({"results": []})
        adapter._client = mock_client

        adapter.fetch(
            adapter_config={"search_queries": ["parking lot {city}"], "fetch_details": False},
            stream="stream_a",
            profile={
                "geography": {"cities": ["Syracuse", "Rochester", "Buffalo"]},
                "exclude_operators": [],
            },
        )
        # 1 query template × 3 cities = 3 API calls
        assert mock_client.places.call_count == 3


# ---------------------------------------------------------------------------
# Deduplicator tests
# ---------------------------------------------------------------------------

class TestDeduplicator:
    def _mock_odoo(self, matches=None):
        odoo = MagicMock()
        odoo.search_duplicate.return_value = matches or []
        return odoo

    def test_no_existing_leads_is_new(self):
        rec = ProspectRecord(partner_name="New Corp", city="Syracuse")
        odoo = self._mock_odoo(matches=[])
        assert is_duplicate(rec, odoo, ["partner_name", "city"]) is False

    def test_existing_match_is_duplicate(self):
        rec = ProspectRecord(partner_name="Acme Corp", city="Syracuse")
        odoo = self._mock_odoo(matches=[{"id": 1, "partner_name": "Acme Corp", "city": "Syracuse"}])
        assert is_duplicate(rec, odoo, ["partner_name", "city"]) is True

    def test_street_mismatch_is_new(self):
        """Same company name in same city but different street → not a dup."""
        rec = ProspectRecord(
            partner_name="Downtown Parking", city="Syracuse", street="100 Main St"
        )
        # Odoo returns a match with a different street
        odoo = self._mock_odoo(matches=[{
            "id": 1,
            "partner_name": "Downtown Parking",
            "city": "Syracuse",
            "street": "200 Water St",
        }])
        assert is_duplicate(rec, odoo, ["partner_name", "city", "street"]) is False

    def test_street_match_is_duplicate(self):
        rec = ProspectRecord(
            partner_name="Downtown Parking", city="Syracuse", street="100 Main St"
        )
        odoo = self._mock_odoo(matches=[{
            "id": 1,
            "partner_name": "Downtown Parking",
            "city": "Syracuse",
            "street": "100 Main St",
        }])
        assert is_duplicate(rec, odoo, ["partner_name", "city", "street"]) is True

    def test_empty_partner_name_skipped(self):
        rec = ProspectRecord(partner_name="")
        odoo = self._mock_odoo()
        assert is_duplicate(rec, odoo, ["partner_name"]) is False
        odoo.search_duplicate.assert_not_called()

    def test_split_partitions_correctly(self):
        records = [
            ProspectRecord(partner_name="New Corp", city="Albany"),
            ProspectRecord(partner_name="Existing Corp", city="Buffalo"),
            ProspectRecord(partner_name="Another New Corp", city="Troy"),
        ]
        odoo = MagicMock()
        odoo.search_duplicate.side_effect = lambda name, city=None: (
            [{"id": 1, "partner_name": name}] if "Existing" in name else []
        )

        new, dupes = split_new_and_duplicate(records, odoo, ["partner_name", "city"])
        assert len(new) == 2
        assert len(dupes) == 1
        assert dupes[0].partner_name == "Existing Corp"

    def test_city_passed_when_in_match_on(self):
        rec = ProspectRecord(partner_name="Test Corp", city="Syracuse")
        odoo = self._mock_odoo()
        is_duplicate(rec, odoo, ["partner_name", "city"])
        odoo.search_duplicate.assert_called_once_with("Test Corp", city="Syracuse")

    def test_city_not_passed_when_not_in_match_on(self):
        rec = ProspectRecord(partner_name="Test Corp", city="Syracuse")
        odoo = self._mock_odoo()
        is_duplicate(rec, odoo, ["partner_name"])
        odoo.search_duplicate.assert_called_once_with("Test Corp", city=None)
