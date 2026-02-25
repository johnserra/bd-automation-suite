"""Tests for Lead Enrichment — Module 3.

All tests are pure unit tests (no live network, Odoo, or Claude calls).
Adapters and main.py are tested via mocking.
"""

import xml.etree.ElementTree as ET
from datetime import date
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Imports under test
# ---------------------------------------------------------------------------
from modules.lead_enrichment.adapters.base import BaseEnrichmentAdapter, EnrichmentResult
from modules.lead_enrichment.adapters.company_website import (
    CompanyWebsiteEnrichmentAdapter,
    _html_to_text,
    _normalize_url,
    _parse_json_object,
)
from modules.lead_enrichment.adapters.trade_data_detail import (
    TradeDataDetailAdapter,
    _company_to_slug,
    _parse_company_page,
)
from modules.lead_enrichment.adapters.news_search import (
    NewsSearchAdapter,
    _fetch_news,
    _parse_rss,
    _strip_tags,
)
from modules.lead_enrichment.adapters.google_maps_detail import (
    GoogleMapsDetailAdapter,
    _infer_business_type,
)
from modules.lead_enrichment.adapters.market_presence_check import (
    MarketPresenceCheckAdapter,
    _find_matching_operator,
)


# ===========================================================================
# Fixtures
# ===========================================================================

def make_lead(**kwargs):
    defaults = {
        "id": 1,
        "name": "Test Lead",
        "partner_name": "Acme Corp",
        "website": "https://acme.com",
        "city": "Syracuse",
        "street": "123 Main St",
        "state_id": [42, "New York"],
        "stage_id": [1, "Research"],
        "x_bd_stream": "stream_c",
        "x_enrichment_status": "pending",
        "description": "",
        "x_data_source": "trade_data",
    }
    defaults.update(kwargs)
    return defaults


# ===========================================================================
# 1. EnrichmentResult tests
# ===========================================================================

class TestEnrichmentResult:
    def test_default_values(self):
        r = EnrichmentResult(source="test", success=True)
        assert r.source == "test"
        assert r.success is True
        assert r.fields_updated == {}
        assert r.description_note == ""
        assert r.error == ""

    def test_all_fields(self):
        r = EnrichmentResult(
            source="my_src",
            success=False,
            fields_updated={"x_foo": "bar"},
            description_note="[Note] something",
            error="network timeout",
        )
        assert r.source == "my_src"
        assert not r.success
        assert r.fields_updated == {"x_foo": "bar"}
        assert r.description_note == "[Note] something"
        assert r.error == "network timeout"

    def test_fields_updated_is_independent(self):
        r1 = EnrichmentResult(source="a", success=True)
        r2 = EnrichmentResult(source="b", success=True)
        r1.fields_updated["x"] = 1
        assert "x" not in r2.fields_updated


# ===========================================================================
# 2. BaseEnrichmentAdapter
# ===========================================================================

class ConcreteAdapter(BaseEnrichmentAdapter):
    name = "test_adapter"

    def enrich(self, lead, fields_to_update, adapter_config):
        return EnrichmentResult(source=self.name, success=True)


class TestBaseEnrichmentAdapter:
    def setup_method(self):
        self.adapter = ConcreteAdapter()
        self.source_list = [
            {"source": "test_adapter", "fields_to_update": ["description", "x_company_size"]},
            {"source": "other_adapter", "fields_to_update": ["x_foo"]},
        ]

    def test_is_source_configured_found(self):
        assert self.adapter.is_source_configured(self.source_list) is True

    def test_is_source_configured_not_found(self):
        other = [{"source": "other_adapter", "fields_to_update": []}]
        assert self.adapter.is_source_configured(other) is False

    def test_get_fields_to_update_found(self):
        fields = self.adapter.get_fields_to_update(self.source_list)
        assert fields == ["description", "x_company_size"]

    def test_get_fields_to_update_not_found(self):
        fields = self.adapter.get_fields_to_update([{"source": "missing", "fields_to_update": []}])
        assert fields == []

    def test_get_fields_to_update_empty_list(self):
        assert self.adapter.get_fields_to_update([]) == []


# ===========================================================================
# 3. CompanyWebsiteEnrichmentAdapter
# ===========================================================================

class TestNormalizeUrl:
    def test_adds_https(self):
        assert _normalize_url("acme.com") == "https://acme.com"

    def test_strips_trailing_slash(self):
        assert _normalize_url("https://acme.com/") == "https://acme.com"

    def test_keeps_http(self):
        assert _normalize_url("http://acme.com") == "http://acme.com"

    def test_empty_string(self):
        assert _normalize_url("") == ""

    def test_whitespace_only(self):
        assert _normalize_url("   ") == ""


class TestHtmlToText:
    def test_strips_script_tags(self):
        html = "<html><script>alert(1)</script><p>Hello</p></html>"
        assert "alert" not in _html_to_text(html)
        assert "Hello" in _html_to_text(html)

    def test_strips_nav_and_footer(self):
        html = "<nav>Navigation</nav><main>Content</main><footer>Footer</footer>"
        result = _html_to_text(html)
        assert "Navigation" not in result
        assert "Content" in result
        assert "Footer" not in result

    def test_collapses_whitespace(self):
        html = "<p>Hello    World</p>"
        result = _html_to_text(html)
        assert "  " not in result


class TestParseJsonObject:
    def test_valid_json_object(self):
        raw = '{"description": "makes widgets", "size": "medium"}'
        result = _parse_json_object(raw)
        assert result == {"description": "makes widgets", "size": "medium"}

    def test_json_with_surrounding_prose(self):
        raw = 'Here is the result: {"description": "bakery"} Hope that helps.'
        result = _parse_json_object(raw)
        assert result["description"] == "bakery"

    def test_no_json_object(self):
        assert _parse_json_object("No JSON here") == {}

    def test_malformed_json(self):
        assert _parse_json_object("{not valid json}") == {}


class TestCompanyWebsiteAdapter:
    def _make_adapter(self, llm_response='{"description": "Makes PET containers", "size": "medium"}'):
        mock_llm = MagicMock()
        mock_llm.complete.return_value = llm_response
        return CompanyWebsiteEnrichmentAdapter(llm_client=mock_llm)

    def test_no_website_returns_failure(self):
        adapter = self._make_adapter()
        lead = make_lead(website="")
        result = adapter.enrich(lead, ["description"], {})
        assert not result.success
        assert "No website" in result.error

    def test_no_pages_fetched_returns_failure(self):
        adapter = self._make_adapter()
        adapter._fetch_text = MagicMock(return_value=None)
        lead = make_lead()
        result = adapter.enrich(lead, ["description"], {})
        assert not result.success
        assert "No pages fetched" in result.error

    def test_description_in_fields_to_update(self):
        adapter = self._make_adapter()
        adapter._fetch_text = MagicMock(return_value="We make food packaging products.")
        lead = make_lead()
        result = adapter.enrich(lead, ["description"], {})
        assert result.success
        assert "[Website]" in result.description_note
        assert "PET containers" in result.description_note

    def test_description_not_in_fields_to_update(self):
        adapter = self._make_adapter()
        adapter._fetch_text = MagicMock(return_value="Some text")
        lead = make_lead()
        result = adapter.enrich(lead, ["x_company_size"], {})
        assert result.success
        assert result.description_note == ""  # not requested

    def test_x_company_size_extracted(self):
        adapter = self._make_adapter()
        adapter._fetch_text = MagicMock(return_value="We are a mid-sized company.")
        lead = make_lead()
        result = adapter.enrich(lead, ["description", "x_company_size"], {})
        assert result.success
        assert result.fields_updated.get("x_company_size") == "medium"

    def test_llm_failure_returns_success_with_empty_fields(self):
        mock_llm = MagicMock()
        mock_llm.complete.side_effect = RuntimeError("API error")
        adapter = CompanyWebsiteEnrichmentAdapter(llm_client=mock_llm)
        adapter._fetch_text = MagicMock(return_value="Some website text")
        lead = make_lead()
        result = adapter.enrich(lead, ["description"], {})
        # Graceful — success=True but no data extracted
        assert result.success
        assert result.description_note == ""

    def test_ssl_error_falls_back_to_http(self):
        import requests as req
        adapter = self._make_adapter()
        call_log = []

        def fake_get(url, **kwargs):
            call_log.append(url)
            if url.startswith("https://"):
                raise req.exceptions.SSLError("SSL")
            mock_resp = MagicMock()
            mock_resp.status_code = 200
            mock_resp.text = "<p>About us</p>"
            mock_resp.raise_for_status = MagicMock()
            return mock_resp

        adapter._session.get = fake_get
        lead = make_lead(website="https://acme.com")
        result = adapter.enrich(lead, ["description"], {})
        # Should have attempted http fallback
        http_calls = [u for u in call_log if u.startswith("http://")]
        assert len(http_calls) > 0


# ===========================================================================
# 4. TradeDataDetailAdapter
# ===========================================================================

class TestCompanyToSlug:
    def test_basic_name(self):
        assert _company_to_slug("Acme Bakery") == "acme-bakery"

    def test_strips_inc(self):
        assert _company_to_slug("Fresh Foods Inc") == "fresh-foods"

    def test_strips_llc(self):
        assert _company_to_slug("ABC LLC") == "abc"

    def test_special_chars(self):
        assert _company_to_slug("Smith & Sons Co.") == "smith-sons"

    def test_lowercase(self):
        assert _company_to_slug("UPPERCASE") == "uppercase"


class TestParseCompanyPage:
    def _make_html(self, shipments="", suppliers=None, countries=""):
        sup_html = ""
        if suppliers:
            for s in suppliers:
                sup_html += f'<a href="/company/{s.lower().replace(" ", "-")}" class="supplier-name">{s}</a>'
        return f"""
        <html><body>
          <p>{shipments} shipments found</p>
          {sup_html}
          <p>From countries: {countries}</p>
        </body></html>
        """

    def test_parses_shipment_count(self):
        html = self._make_html(shipments="1,234")
        result = _parse_company_page(html)
        assert result is not None
        assert result["shipment_count"] == "1234"

    def test_parses_suppliers(self):
        html = self._make_html(suppliers=["Sunrise Foods", "Global Pack"])
        result = _parse_company_page(html)
        assert result is not None
        assert "Sunrise Foods" in result["suppliers"]

    def test_parses_overseas_countries(self):
        html = self._make_html(shipments="50", countries="CN TR")
        result = _parse_company_page(html)
        assert result is not None
        assert "CN" in result["countries"] or "TR" in result["countries"]

    def test_returns_none_for_empty_page(self):
        html = "<html><body><p>No relevant data here</p></body></html>"
        assert _parse_company_page(html) is None


class TestTradeDataDetailAdapter:
    def test_no_company_name_returns_failure(self):
        adapter = TradeDataDetailAdapter()
        lead = make_lead(partner_name="", name="")
        result = adapter.enrich(lead, ["x_already_importing"], {})
        assert not result.success
        assert "No company name" in result.error

    def test_data_not_found_returns_failure(self):
        adapter = TradeDataDetailAdapter()
        adapter._fetch_company_page = MagicMock(return_value=None)
        adapter._fetch_via_search = MagicMock(return_value=None)
        lead = make_lead()
        result = adapter.enrich(lead, ["x_already_importing"], {})
        assert not result.success
        assert "No trade data found" in result.error

    def test_sets_x_already_importing(self):
        adapter = TradeDataDetailAdapter()
        adapter._fetch_company_page = MagicMock(return_value={
            "suppliers": ["Supplier A"],
            "countries": ["CN"],
            "shipment_count": "50",
        })
        lead = make_lead()
        result = adapter.enrich(lead, ["x_already_importing", "x_current_supplier", "x_import_source_country"], {})
        assert result.success
        assert result.fields_updated["x_already_importing"] is True
        assert "Supplier A" in result.fields_updated["x_current_supplier"]
        assert result.fields_updated["x_import_source_country"] == "CN"

    def test_description_note_includes_shipment_count(self):
        adapter = TradeDataDetailAdapter()
        adapter._fetch_company_page = MagicMock(return_value={
            "suppliers": ["XYZ Foods"],
            "countries": ["TR"],
            "shipment_count": "100",
        })
        lead = make_lead()
        result = adapter.enrich(lead, ["x_already_importing", "x_current_supplier", "x_import_source_country"], {})
        assert "Shipments: 100" in result.description_note
        assert "[TradeData]" in result.description_note

    def test_fields_filtered_by_fields_to_update(self):
        adapter = TradeDataDetailAdapter()
        adapter._fetch_company_page = MagicMock(return_value={
            "suppliers": ["Corp X"],
            "countries": ["CN"],
            "shipment_count": "10",
        })
        lead = make_lead()
        # Only request x_already_importing, not supplier/country
        result = adapter.enrich(lead, ["x_already_importing"], {})
        assert "x_already_importing" in result.fields_updated
        assert "x_current_supplier" not in result.fields_updated
        assert "x_import_source_country" not in result.fields_updated

    def test_search_fallback_used_when_direct_fails(self):
        adapter = TradeDataDetailAdapter()
        direct_data = {"suppliers": ["Fallback Supplier"], "countries": ["TR"], "shipment_count": "5"}
        adapter._fetch_company_page = MagicMock(return_value=None)
        adapter._fetch_via_search = MagicMock(return_value=direct_data)
        lead = make_lead()
        result = adapter.enrich(lead, ["x_already_importing", "x_current_supplier"], {})
        assert result.success
        adapter._fetch_via_search.assert_called_once()

    def test_no_description_note_when_no_fields_returned(self):
        adapter = TradeDataDetailAdapter()
        adapter._fetch_company_page = MagicMock(return_value={
            "suppliers": [],
            "countries": [],
            "shipment_count": None,
        })
        lead = make_lead()
        # This will return None from _fetch_company_page effectively since parse returns None
        # but we mocked it — let's test the note when data has nothing useful
        adapter._fetch_company_page.return_value = {"suppliers": [], "countries": [], "shipment_count": None}
        result = adapter.enrich(lead, ["x_already_importing"], {})
        # x_already_importing is always set if data returned, even empty data
        # But _parse returns None for empty — here adapter mock returns non-None so it proceeds
        assert result.fields_updated.get("x_already_importing") is True
        assert result.description_note == ""


# ===========================================================================
# 5. NewsSearchAdapter
# ===========================================================================

class TestStripTags:
    def test_removes_html_tags(self):
        assert _strip_tags("<b>Hello</b> <i>World</i>") == "Hello World"

    def test_no_tags(self):
        assert _strip_tags("Plain text") == "Plain text"

    def test_empty_string(self):
        assert _strip_tags("") == ""


class TestParseRss:
    def _make_rss(self, items):
        items_xml = ""
        for item in items:
            items_xml += f"""
            <item>
                <title>{item.get('title', '')}</title>
                <description>{item.get('desc', '')}</description>
                <link>{item.get('link', '')}</link>
            </item>"""
        return f"""<?xml version="1.0"?>
        <rss version="2.0"><channel>{items_xml}</channel></rss>"""

    def test_parses_items(self):
        xml = self._make_rss([
            {"title": "Acme Expands", "desc": "Big news", "link": "https://example.com/1"},
            {"title": "Acme Reports", "desc": "More news", "link": "https://example.com/2"},
        ])
        articles = _parse_rss(xml)
        assert len(articles) == 2
        assert articles[0]["title"] == "Acme Expands"
        assert articles[0]["snippet"] == "Big news"

    def test_invalid_xml_returns_empty(self):
        assert _parse_rss("not xml at all <<<") == []

    def test_empty_items_skipped(self):
        xml = self._make_rss([{"title": "", "desc": "no title"}])
        articles = _parse_rss(xml)
        assert articles == []


class TestNewsSearchAdapter:
    def _make_adapter(self, llm_response="Company expanding to new markets."):
        mock_llm = MagicMock()
        mock_llm.complete.return_value = llm_response
        return NewsSearchAdapter(llm_client=mock_llm)

    def test_no_company_name_returns_failure(self):
        adapter = self._make_adapter()
        lead = make_lead(partner_name="", name="")
        result = adapter.enrich(lead, ["description"], {})
        assert not result.success
        assert "No company name" in result.error

    def test_description_not_in_fields_returns_early_success(self):
        adapter = self._make_adapter()
        lead = make_lead()
        result = adapter.enrich(lead, ["x_company_size"], {})
        assert result.success
        assert result.description_note == ""

    def test_no_articles_found_returns_success_empty_note(self):
        adapter = self._make_adapter()
        with patch("modules.lead_enrichment.adapters.news_search._fetch_news", return_value=[]):
            result = adapter.enrich(lead=make_lead(), fields_to_update=["description"], adapter_config={})
        assert result.success
        assert result.description_note == ""

    def test_articles_summarized_into_note(self):
        adapter = self._make_adapter("Acme is opening a new facility in Ohio.")
        articles = [{"title": "Acme Expands", "snippet": "New facility planned."}]
        with patch("modules.lead_enrichment.adapters.news_search._fetch_news", return_value=articles):
            result = adapter.enrich(make_lead(), ["description"], {})
        assert result.success
        assert "[News]" in result.description_note
        assert "Ohio" in result.description_note

    def test_llm_returns_no_relevant_news_gives_empty_note(self):
        adapter = self._make_adapter("No recent relevant news found.")
        articles = [{"title": "Acme", "snippet": "Generic article."}]
        with patch("modules.lead_enrichment.adapters.news_search._fetch_news", return_value=articles):
            result = adapter.enrich(make_lead(), ["description"], {})
        assert result.success
        assert result.description_note == ""

    def test_llm_failure_returns_empty_note(self):
        mock_llm = MagicMock()
        mock_llm.complete.side_effect = RuntimeError("API down")
        adapter = NewsSearchAdapter(llm_client=mock_llm)
        articles = [{"title": "Some News", "snippet": "text"}]
        with patch("modules.lead_enrichment.adapters.news_search._fetch_news", return_value=articles):
            result = adapter.enrich(make_lead(), ["description"], {})
        assert result.success
        assert result.description_note == ""


# ===========================================================================
# 6. GoogleMapsDetailAdapter
# ===========================================================================

class TestInferBusinessType:
    def test_parking_type(self):
        assert _infer_business_type(["parking", "establishment"]) == "parking"

    def test_restaurant_type(self):
        assert _infer_business_type(["restaurant", "food"]) == "restaurant"

    def test_unknown_type_returns_none(self):
        assert _infer_business_type(["establishment", "point_of_interest"]) is None

    def test_empty_types(self):
        assert _infer_business_type([]) is None

    def test_prefers_first_match(self):
        # parking comes before restaurant in TYPE_MAP traversal (list order)
        result = _infer_business_type(["parking", "restaurant"])
        assert result == "parking"


class TestGoogleMapsDetailAdapter:
    _DEFAULT_PLACE = {
        "name": "Acme Corp",
        "rating": 4.2,
        "user_ratings_total": 38,
        "types": ["food", "establishment"],
        "formatted_address": "123 Main St",
    }
    _DEFAULT_RESULTS = [{"place_id": "ChIJ123", "name": "Acme Corp"}]

    def _make_adapter_with_mock(self, search_results=None, place_details=None):
        mock_gmaps = MagicMock()
        results = self._DEFAULT_RESULTS if search_results is None else search_results
        mock_gmaps.places.return_value = {"results": results}
        details = self._DEFAULT_PLACE if place_details is None else place_details
        mock_gmaps.place.return_value = {"result": details}
        return GoogleMapsDetailAdapter(gmaps_client=mock_gmaps)

    def test_no_company_name_returns_failure(self):
        adapter = self._make_adapter_with_mock()
        lead = make_lead(partner_name="", name="")
        result = adapter.enrich(lead, ["description"], {})
        assert not result.success

    def test_no_api_key_returns_failure(self):
        adapter = GoogleMapsDetailAdapter(gmaps_client=None)
        with patch.dict("os.environ", {}, clear=True):
            # No API key in environment
            import os
            os.environ.pop("GOOGLE_MAPS_API_KEY", None)
            result = adapter.enrich(make_lead(), ["description"], {})
        assert not result.success
        assert "GOOGLE_MAPS_API_KEY" in result.error

    def test_empty_search_results_returns_failure(self):
        adapter = self._make_adapter_with_mock(search_results=[])
        result = adapter.enrich(make_lead(), ["description"], {})
        assert not result.success
        assert "No Google Maps results" in result.error

    def test_rating_added_to_description_note(self):
        adapter = self._make_adapter_with_mock()
        result = adapter.enrich(make_lead(), ["description"], {})
        assert result.success
        assert "4.2/5" in result.description_note
        assert "[Google Maps]" in result.description_note

    def test_business_type_extracted(self):
        adapter = self._make_adapter_with_mock(
            place_details={
                "name": "Test Parking",
                "rating": 3.5,
                "user_ratings_total": 10,
                "types": ["parking", "establishment"],
                "formatted_address": "456 Park Ave",
            }
        )
        result = adapter.enrich(make_lead(), ["x_business_type", "description"], {})
        assert result.success
        assert result.fields_updated.get("x_business_type") == "parking"

    def test_description_not_in_fields_no_note(self):
        adapter = self._make_adapter_with_mock()
        result = adapter.enrich(make_lead(), ["x_business_type"], {})
        assert result.success
        assert result.description_note == ""

    def test_places_search_exception_returns_failure(self):
        mock_gmaps = MagicMock()
        mock_gmaps.places.side_effect = Exception("Network error")
        adapter = GoogleMapsDetailAdapter(gmaps_client=mock_gmaps)
        result = adapter.enrich(make_lead(), ["description"], {})
        assert not result.success


# ===========================================================================
# 7. MarketPresenceCheckAdapter
# ===========================================================================

class TestFindMatchingOperator:
    def test_exact_match(self):
        results = [{"name": "Operator A Parking"}, {"name": "Joe's Lot"}]
        assert _find_matching_operator(results, ["Operator A"]) == "Operator A Parking"

    def test_case_insensitive(self):
        results = [{"name": "operator b downtown"}]
        assert _find_matching_operator(results, ["Operator B"]) == "operator b downtown"

    def test_no_match_returns_none(self):
        results = [{"name": "City Lot"}, {"name": "Main Street Parking"}]
        assert _find_matching_operator(results, ["Operator A", "Operator B"]) is None

    def test_empty_results(self):
        assert _find_matching_operator([], ["Operator A"]) is None


class TestMarketPresenceCheckAdapter:
    def _make_adapter(self, search_results=None):
        mock_gmaps = MagicMock()
        mock_gmaps.places.return_value = {
            "results": search_results or []
        }
        return MarketPresenceCheckAdapter(gmaps_client=mock_gmaps)

    def test_field_not_requested_returns_early_success(self):
        adapter = self._make_adapter()
        result = adapter.enrich(make_lead(), ["description"], {})
        assert result.success
        assert result.fields_updated == {}

    def test_no_address_or_company_returns_failure(self):
        adapter = self._make_adapter()
        lead = make_lead(street="", city="", partner_name="")
        result = adapter.enrich(lead, ["x_current_operator"], {})
        assert not result.success

    def test_operator_found_sets_field(self):
        adapter = self._make_adapter(search_results=[
            {"name": "Operator A Parking Management"},
            {"name": "City Parking"},
        ])
        result = adapter.enrich(make_lead(), ["x_current_operator"], {"known_operators": ["Operator A"]})
        assert result.success
        assert result.fields_updated["x_current_operator"] == "Operator A Parking Management"
        assert "[MarketPresence]" in result.description_note

    def test_no_operator_found_returns_success_no_field(self):
        adapter = self._make_adapter(search_results=[
            {"name": "Random Lot"},
        ])
        result = adapter.enrich(make_lead(), ["x_current_operator"], {"known_operators": ["Operator A"]})
        assert result.success
        assert "x_current_operator" not in result.fields_updated
        assert result.description_note == ""

    def test_uses_default_known_operators_when_none_in_config(self):
        adapter = self._make_adapter(search_results=[
            {"name": "Operator B of Syracuse"},
        ])
        result = adapter.enrich(make_lead(), ["x_current_operator"], {})
        assert result.success
        assert result.fields_updated.get("x_current_operator") == "Operator B of Syracuse"

    def test_places_exception_returns_failure(self):
        mock_gmaps = MagicMock()
        mock_gmaps.places.side_effect = Exception("API error")
        adapter = MarketPresenceCheckAdapter(gmaps_client=mock_gmaps)
        result = adapter.enrich(make_lead(), ["x_current_operator"], {})
        assert not result.success

    def test_no_api_key_returns_failure(self):
        adapter = MarketPresenceCheckAdapter(gmaps_client=None)
        import os
        os.environ.pop("GOOGLE_MAPS_API_KEY", None)
        with patch.dict("os.environ", {}, clear=True):
            result = adapter.enrich(make_lead(), ["x_current_operator"], {})
        assert not result.success


# ===========================================================================
# 8. main.py — fetch_leads_to_enrich + run()
# ===========================================================================

class TestFetchLeadsToEnrich:
    def test_calls_search_leads_with_correct_domain(self):
        mock_odoo = MagicMock()
        mock_odoo.search_leads.return_value = []

        from modules.lead_enrichment.main import fetch_leads_to_enrich
        fetch_leads_to_enrich(mock_odoo, stream_filter=None, limit=None)

        call_args = mock_odoo.search_leads.call_args
        domain = call_args[0][0]
        # Should filter for pending/partial enrichment status
        status_filter = next(f for f in domain if f[0] == "x_enrichment_status")
        assert status_filter[1] == "in"
        assert "pending" in status_filter[2]

    def test_stream_filter_added_to_domain(self):
        mock_odoo = MagicMock()
        mock_odoo.search_leads.return_value = []

        from modules.lead_enrichment.main import fetch_leads_to_enrich
        fetch_leads_to_enrich(mock_odoo, stream_filter="stream_c", limit=5)

        call_args = mock_odoo.search_leads.call_args
        domain = call_args[0][0]
        stream_filter = next(
            (f for f in domain if isinstance(f, list) and f[0] == "x_bd_stream"), None
        )
        assert stream_filter is not None
        assert stream_filter[2] == "stream_c"


class TestRunOrchestrator:
    def _setup_mocks(self, leads=None):
        mock_odoo = MagicMock()
        mock_odoo.search_leads.return_value = leads or []
        mock_odoo.update_lead.return_value = True
        return mock_odoo

    def test_no_leads_returns_zeros(self):
        from modules.lead_enrichment.main import run
        mock_odoo = self._setup_mocks(leads=[])

        with patch("modules.lead_enrichment.main.OdooClient.from_env", return_value=mock_odoo), \
             patch("modules.lead_enrichment.main.load_config", return_value={
                 "enrichment_sources": {"stream_c": []}
             }):
            result = run()

        assert result["total"] == 0
        assert result["enriched"] == 0

    def test_unknown_stream_lead_is_skipped(self):
        from modules.lead_enrichment.main import run
        lead = make_lead(x_bd_stream="unknown_stream")
        mock_odoo = self._setup_mocks(leads=[lead])

        with patch("modules.lead_enrichment.main.OdooClient.from_env", return_value=mock_odoo), \
             patch("modules.lead_enrichment.main.load_config", return_value={
                 "enrichment_sources": {}
             }):
            result = run()

        assert result["skipped"] == 1
        assert result["enriched"] == 0
        mock_odoo.update_lead.assert_not_called()

    def test_successful_enrichment_marks_complete(self):
        from modules.lead_enrichment.main import run, ADAPTER_REGISTRY

        lead = make_lead(x_bd_stream="stream_c")
        mock_odoo = self._setup_mocks(leads=[lead])

        mock_adapter = MagicMock()
        mock_adapter.enrich.return_value = EnrichmentResult(
            source="company_website",
            success=True,
            fields_updated={"x_company_size": "medium"},
            description_note="[Website] Makes food containers.",
        )

        config = {
            "enrichment_sources": {
                "stream_c": [
                    {"source": "company_website", "fields_to_update": ["description", "x_company_size"]}
                ]
            }
        }

        patched_registry = {"company_website": mock_adapter}

        with patch("modules.lead_enrichment.main.OdooClient.from_env", return_value=mock_odoo), \
             patch("modules.lead_enrichment.main.load_config", return_value=config), \
             patch("modules.lead_enrichment.main.ADAPTER_REGISTRY", patched_registry):
            result = run()

        assert result["enriched"] == 1
        assert result["partial"] == 0

        update_call = mock_odoo.update_lead.call_args[0]
        written_fields = update_call[1]
        assert written_fields["x_enrichment_status"] == "complete"
        assert written_fields["x_company_size"] == "medium"

    def test_partial_enrichment_on_mixed_results(self):
        from modules.lead_enrichment.main import run

        lead = make_lead(x_bd_stream="stream_c")
        mock_odoo = self._setup_mocks(leads=[lead])

        success_adapter = MagicMock()
        success_adapter.enrich.return_value = EnrichmentResult(
            source="company_website", success=True,
            fields_updated={"x_company_size": "large"}, description_note="",
        )
        fail_adapter = MagicMock()
        fail_adapter.enrich.return_value = EnrichmentResult(
            source="trade_data_detail", success=False, error="not found",
        )

        config = {
            "enrichment_sources": {
                "stream_c": [
                    {"source": "company_website", "fields_to_update": ["x_company_size"]},
                    {"source": "trade_data_detail", "fields_to_update": ["x_already_importing"]},
                ]
            }
        }
        patched_registry = {"company_website": success_adapter, "trade_data_detail": fail_adapter}

        with patch("modules.lead_enrichment.main.OdooClient.from_env", return_value=mock_odoo), \
             patch("modules.lead_enrichment.main.load_config", return_value=config), \
             patch("modules.lead_enrichment.main.ADAPTER_REGISTRY", patched_registry):
            result = run()

        assert result["partial"] == 1
        assert result["enriched"] == 0
        update_call = mock_odoo.update_lead.call_args[0]
        assert update_call[1]["x_enrichment_status"] == "partial"

    def test_description_notes_appended_to_existing(self):
        from modules.lead_enrichment.main import run

        lead = make_lead(x_bd_stream="stream_c", description="Existing note.")
        mock_odoo = self._setup_mocks(leads=[lead])

        mock_adapter = MagicMock()
        mock_adapter.enrich.return_value = EnrichmentResult(
            source="news_search", success=True,
            description_note="[News] Company expanding.",
        )

        config = {
            "enrichment_sources": {
                "stream_c": [
                    {"source": "news_search", "fields_to_update": ["description"]}
                ]
            }
        }
        patched_registry = {"news_search": mock_adapter}

        with patch("modules.lead_enrichment.main.OdooClient.from_env", return_value=mock_odoo), \
             patch("modules.lead_enrichment.main.load_config", return_value=config), \
             patch("modules.lead_enrichment.main.ADAPTER_REGISTRY", patched_registry):
            run()

        update_call = mock_odoo.update_lead.call_args[0]
        desc = update_call[1]["description"]
        assert "Existing note." in desc
        assert "[News] Company expanding." in desc

    def test_dry_run_does_not_call_update_lead(self):
        from modules.lead_enrichment.main import run

        lead = make_lead(x_bd_stream="stream_c")
        mock_odoo = self._setup_mocks(leads=[lead])

        mock_adapter = MagicMock()
        mock_adapter.enrich.return_value = EnrichmentResult(
            source="company_website", success=True,
            fields_updated={"x_company_size": "medium"}, description_note="",
        )

        config = {
            "enrichment_sources": {
                "stream_c": [
                    {"source": "company_website", "fields_to_update": ["x_company_size"]}
                ]
            }
        }
        patched_registry = {"company_website": mock_adapter}

        with patch("modules.lead_enrichment.main.OdooClient.from_env", return_value=mock_odoo), \
             patch("modules.lead_enrichment.main.load_config", return_value=config), \
             patch("modules.lead_enrichment.main.ADAPTER_REGISTRY", patched_registry):
            result = run(dry_run=True)

        mock_odoo.update_lead.assert_not_called()
        assert result["enriched"] == 1

    def test_adapter_exception_counted_as_error(self):
        from modules.lead_enrichment.main import run

        lead = make_lead(x_bd_stream="stream_c")
        mock_odoo = self._setup_mocks(leads=[lead])

        exploding_adapter = MagicMock()
        exploding_adapter.enrich.side_effect = RuntimeError("Boom!")

        config = {
            "enrichment_sources": {
                "stream_c": [
                    {"source": "company_website", "fields_to_update": ["description"]}
                ]
            }
        }
        patched_registry = {"company_website": exploding_adapter}

        with patch("modules.lead_enrichment.main.OdooClient.from_env", return_value=mock_odoo), \
             patch("modules.lead_enrichment.main.load_config", return_value=config), \
             patch("modules.lead_enrichment.main.ADAPTER_REGISTRY", patched_registry):
            result = run()

        assert result["errors"] == 1

    def test_unknown_source_name_skipped_gracefully(self):
        from modules.lead_enrichment.main import run

        lead = make_lead(x_bd_stream="stream_c")
        mock_odoo = self._setup_mocks(leads=[lead])

        config = {
            "enrichment_sources": {
                "stream_c": [
                    {"source": "linkedin_company", "fields_to_update": ["x_company_linkedin"]},
                ]
            }
        }
        # linkedin_company not in registry — should be skipped, no error
        with patch("modules.lead_enrichment.main.OdooClient.from_env", return_value=mock_odoo), \
             patch("modules.lead_enrichment.main.load_config", return_value=config), \
             patch("modules.lead_enrichment.main.ADAPTER_REGISTRY", {}):
            result = run()

        assert result["errors"] == 0
        # Lead had some attempt (skipped source), should be marked partial
        # update_lead IS called with x_enrichment_date set
        mock_odoo.update_lead.assert_called_once()
        fields = mock_odoo.update_lead.call_args[0][1]
        assert "x_enrichment_date" in fields

    def test_enrichment_date_always_set(self):
        from modules.lead_enrichment.main import run

        lead = make_lead(x_bd_stream="stream_c")
        mock_odoo = self._setup_mocks(leads=[lead])

        mock_adapter = MagicMock()
        mock_adapter.enrich.return_value = EnrichmentResult(
            source="company_website", success=True, fields_updated={}, description_note=""
        )

        config = {
            "enrichment_sources": {
                "stream_c": [
                    {"source": "company_website", "fields_to_update": ["description"]}
                ]
            }
        }
        patched_registry = {"company_website": mock_adapter}

        with patch("modules.lead_enrichment.main.OdooClient.from_env", return_value=mock_odoo), \
             patch("modules.lead_enrichment.main.load_config", return_value=config), \
             patch("modules.lead_enrichment.main.ADAPTER_REGISTRY", patched_registry):
            run()

        fields = mock_odoo.update_lead.call_args[0][1]
        assert "x_enrichment_date" in fields
        assert fields["x_enrichment_date"] == date.today().isoformat()

    def test_update_lead_exception_increments_errors(self):
        from modules.lead_enrichment.main import run

        lead = make_lead(x_bd_stream="stream_c")
        mock_odoo = self._setup_mocks(leads=[lead])
        mock_odoo.update_lead.side_effect = Exception("Odoo write failed")

        mock_adapter = MagicMock()
        mock_adapter.enrich.return_value = EnrichmentResult(
            source="company_website", success=True, fields_updated={}, description_note=""
        )

        config = {
            "enrichment_sources": {
                "stream_c": [
                    {"source": "company_website", "fields_to_update": ["description"]}
                ]
            }
        }
        patched_registry = {"company_website": mock_adapter}

        with patch("modules.lead_enrichment.main.OdooClient.from_env", return_value=mock_odoo), \
             patch("modules.lead_enrichment.main.load_config", return_value=config), \
             patch("modules.lead_enrichment.main.ADAPTER_REGISTRY", patched_registry):
            result = run()

        assert result["errors"] == 1
