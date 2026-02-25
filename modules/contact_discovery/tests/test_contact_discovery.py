"""Unit tests for contact_discovery — ranker, finders, queue, domain inference."""

import csv
import json
import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest

from modules.contact_discovery.finders.base import ContactCandidate
from modules.contact_discovery.finders.hunter import (
    HunterFinder,
    _company_name_to_domain,
    _infer_domain,
)
from modules.contact_discovery.finders.website import (
    WebsiteContactFinder,
    _html_to_text,
    _normalize_url,
    _parse_llm_response,
)
from modules.contact_discovery.linkedin_queue import (
    format_queue_summary,
    make_linkedin_search_url,
    write_queue_file,
)
from modules.contact_discovery.ranker import best_candidate, rank_candidates


# ---------------------------------------------------------------------------
# ContactCandidate helpers
# ---------------------------------------------------------------------------

class TestContactCandidate:
    def test_is_actionable_with_name(self):
        c = ContactCandidate(name="Jane Doe")
        assert c.is_actionable() is True

    def test_is_actionable_empty_name(self):
        c = ContactCandidate(name="")
        assert c.is_actionable() is False

    def test_has_contact_info_email(self):
        c = ContactCandidate(name="Jane", email="jane@co.com")
        assert c.has_contact_info() is True

    def test_has_contact_info_linkedin(self):
        c = ContactCandidate(name="Jane", linkedin_url="https://linkedin.com/in/jane")
        assert c.has_contact_info() is True

    def test_has_contact_info_neither(self):
        c = ContactCandidate(name="Jane")
        assert c.has_contact_info() is False


# ---------------------------------------------------------------------------
# Ranker
# ---------------------------------------------------------------------------

PACKAGING_TITLES = [
    "Procurement Manager",
    "Purchasing Manager",
    "VP Supply Chain",
    "Operations Director",
    "Owner",
    "President",
]

PARKING_TITLES = [
    "Property Manager",
    "Owner",
    "Director of Real Estate",
    "Asset Manager",
]


class TestRankCandidates:
    def _c(self, name, title=None, email=None, linkedin=None, confidence=0.5, source="website"):
        return ContactCandidate(
            name=name, title=title, email=email,
            linkedin_url=linkedin, confidence=confidence, source=source,
        )

    def test_exact_title_match_wins(self):
        candidates = [
            self._c("Alice", title="Procurement Manager"),
            self._c("Bob",   title="President"),
        ]
        ranked = rank_candidates(candidates, PACKAGING_TITLES)
        assert ranked[0].name == "Alice"   # Procurement Manager is index 0

    def test_higher_priority_title_beats_lower(self):
        candidates = [
            self._c("Alice", title="Owner"),          # index 4
            self._c("Bob",   title="Purchasing Manager"),  # index 1
        ]
        ranked = rank_candidates(candidates, PACKAGING_TITLES)
        assert ranked[0].name == "Bob"

    def test_email_breaks_title_tie(self):
        candidates = [
            self._c("Alice", title="Procurement Manager", email=None),
            self._c("Bob",   title="Procurement Manager", email="bob@co.com"),
        ]
        ranked = rank_candidates(candidates, PACKAGING_TITLES)
        assert ranked[0].name == "Bob"   # has email

    def test_linkedin_breaks_tie_after_email(self):
        candidates = [
            self._c("Alice", title="Owner", linkedin=None),
            self._c("Bob",   title="Owner", linkedin="https://linkedin.com/in/bob"),
        ]
        ranked = rank_candidates(candidates, PARKING_TITLES)
        assert ranked[0].name == "Bob"

    def test_word_subset_match(self):
        # "Supply Chain Manager" → matches "VP Supply Chain" partially
        candidates = [
            self._c("Alice", title="Supply Chain Manager"),
            self._c("Bob",   title="Receptionist"),
        ]
        ranked = rank_candidates(candidates, PACKAGING_TITLES)
        assert ranked[0].name == "Alice"

    def test_no_title_match_scores_lowest(self):
        candidates = [
            self._c("Alice", title="Receptionist"),
            self._c("Bob",   title="Procurement Manager"),
        ]
        ranked = rank_candidates(candidates, PACKAGING_TITLES)
        assert ranked[0].name == "Bob"

    def test_empty_candidates_returns_empty(self):
        assert rank_candidates([], PACKAGING_TITLES) == []

    def test_empty_titles_uses_confidence_only(self):
        candidates = [
            self._c("Alice", confidence=0.3),
            self._c("Bob",   confidence=0.9),
        ]
        ranked = rank_candidates(candidates, [])
        assert ranked[0].name == "Bob"

    def test_case_insensitive_exact_match(self):
        candidates = [
            self._c("Alice", title="procurement manager"),  # lowercase
            self._c("Bob",   title="President"),
        ]
        ranked = rank_candidates(candidates, PACKAGING_TITLES)
        assert ranked[0].name == "Alice"


class TestBestCandidate:
    def test_returns_best(self):
        candidates = [
            ContactCandidate(name="Alice", title="Procurement Manager"),
            ContactCandidate(name="Bob",   title="Owner"),
        ]
        result = best_candidate(candidates, PACKAGING_TITLES)
        assert result is not None
        assert result.name == "Alice"

    def test_returns_none_for_empty(self):
        assert best_candidate([], PACKAGING_TITLES) is None

    def test_skips_non_actionable(self):
        candidates = [ContactCandidate(name="")]
        assert best_candidate(candidates, PACKAGING_TITLES) is None


# ---------------------------------------------------------------------------
# Website finder helpers
# ---------------------------------------------------------------------------

class TestNormalizeUrl:
    def test_adds_https(self):
        assert _normalize_url("acme.com") == "https://acme.com"

    def test_strips_trailing_slash(self):
        assert _normalize_url("https://acme.com/") == "https://acme.com"

    def test_preserves_https(self):
        assert _normalize_url("https://acme.com") == "https://acme.com"

    def test_preserves_http(self):
        assert _normalize_url("http://acme.com") == "http://acme.com"

    def test_empty_string(self):
        assert _normalize_url("") == ""

    def test_whitespace_stripped(self):
        assert _normalize_url("  acme.com  ") == "https://acme.com"


class TestHtmlToText:
    def test_strips_tags(self):
        html = "<h1>Hello</h1><p>World</p>"
        assert "Hello" in _html_to_text(html)
        assert "<h1>" not in _html_to_text(html)

    def test_removes_script(self):
        html = "<script>var x=1;</script><p>Content</p>"
        text = _html_to_text(html)
        assert "var x=1" not in text
        assert "Content" in text

    def test_removes_nav(self):
        html = "<nav>Menu items</nav><main>Main content</main>"
        text = _html_to_text(html)
        assert "Menu items" not in text
        assert "Main content" in text

    def test_collapses_whitespace(self):
        html = "<p>Hello    World</p>"
        text = _html_to_text(html)
        assert "  " not in text


class TestParseLlmResponse:
    def test_valid_json_list(self):
        raw = '[{"name":"Jane Doe","title":"Procurement Manager","email":"jane@co.com"}]'
        candidates = _parse_llm_response(raw, source="website")
        assert len(candidates) == 1
        assert candidates[0].name == "Jane Doe"
        assert candidates[0].title == "Procurement Manager"
        assert candidates[0].email == "jane@co.com"
        assert candidates[0].source == "website"

    def test_json_embedded_in_prose(self):
        raw = 'Here are the contacts: [{"name":"Bob Smith","title":"Owner"}] Hope that helps!'
        candidates = _parse_llm_response(raw, source="website")
        assert len(candidates) == 1
        assert candidates[0].name == "Bob Smith"

    def test_empty_json_list(self):
        assert _parse_llm_response("[]", source="website") == []

    def test_no_json_returns_empty(self):
        assert _parse_llm_response("No contacts found.", source="website") == []

    def test_missing_name_skipped(self):
        raw = '[{"title":"Manager","email":"test@co.com"}]'
        candidates = _parse_llm_response(raw, source="website")
        assert candidates == []

    def test_multiple_contacts(self):
        raw = json.dumps([
            {"name": "Alice", "title": "Procurement Manager"},
            {"name": "Bob",   "title": "Owner", "email": "bob@co.com"},
        ])
        candidates = _parse_llm_response(raw, source="website")
        assert len(candidates) == 2

    def test_confidence_set(self):
        raw = '[{"name":"Alice","title":"Manager"}]'
        candidates = _parse_llm_response(raw, source="website")
        assert candidates[0].confidence == 0.7


class TestWebsiteFinderIntegration:
    """Integration tests with mocked HTTP and LLM."""

    TEAM_HTML = """
    <html><body>
      <main>
        <h2>Our Team</h2>
        <div class="person">
          <h3>Jane Doe</h3>
          <p>Procurement Manager</p>
          <a href="mailto:jane@acmebakery.com">jane@acmebakery.com</a>
        </div>
        <div class="person">
          <h3>Bob Smith</h3>
          <p>CEO</p>
        </div>
      </main>
    </body></html>
    """

    def test_find_returns_candidates_from_llm(self):
        finder = WebsiteContactFinder()
        finder._fetch_page_text = MagicMock(return_value="Jane Doe Procurement Manager")
        mock_llm = MagicMock()
        mock_llm.complete.return_value = (
            '[{"name":"Jane Doe","title":"Procurement Manager","email":"jane@acme.com"}]'
        )
        finder._llm = mock_llm

        lead = {
            "partner_name": "Acme Bakery", "website": "https://acmebakery.com",
            "id": 1,
        }
        candidates = finder.find(
            lead,
            priority_titles=["Procurement Manager", "Owner"],
            finder_config={"pages_to_check": ["/about", "/team"]},
        )
        assert len(candidates) == 1
        assert candidates[0].name == "Jane Doe"
        assert candidates[0].source == "website"

    def test_no_website_returns_empty(self):
        finder = WebsiteContactFinder()
        lead = {"partner_name": "Acme Bakery", "website": None, "id": 1}
        result = finder.find(lead, ["Procurement Manager"], {})
        assert result == []

    def test_http_failure_returns_empty(self):
        import requests as _requests
        finder = WebsiteContactFinder()
        finder._fetch_page_text = MagicMock(return_value=None)
        finder._llm = MagicMock()

        lead = {"partner_name": "Acme Bakery", "website": "https://acmebakery.com", "id": 1}
        result = finder.find(
            lead, ["Procurement Manager"], {"pages_to_check": ["/team"]}
        )
        # All pages returned None, so no LLM call, empty result
        assert result == []
        finder._llm.complete.assert_not_called()

    def test_llm_failure_returns_empty(self):
        finder = WebsiteContactFinder()
        finder._fetch_page_text = MagicMock(return_value="Some team content here")
        mock_llm = MagicMock()
        mock_llm.complete.side_effect = Exception("API error")
        finder._llm = mock_llm

        lead = {"partner_name": "Acme Bakery", "website": "https://acmebakery.com", "id": 1}
        result = finder.find(lead, ["Procurement Manager"], {"pages_to_check": ["/team"]})
        assert result == []


# ---------------------------------------------------------------------------
# Hunter.io helpers
# ---------------------------------------------------------------------------

class TestDomainInference:
    def test_from_website_url(self):
        lead = {"website": "https://www.acmebakery.com", "partner_name": "Acme Bakery"}
        assert _infer_domain(lead) == "acmebakery.com"

    def test_strips_www(self):
        lead = {"website": "https://www.freshfoods.com", "partner_name": "Fresh Foods"}
        assert _infer_domain(lead) == "freshfoods.com"

    def test_fallback_to_company_name(self):
        lead = {"website": None, "partner_name": "Fresh Foods Co"}
        domain = _infer_domain(lead)
        assert domain == "freshfoods.com"

    def test_website_without_scheme(self):
        lead = {"website": "acmebakery.com", "partner_name": "Acme Bakery"}
        assert _infer_domain(lead) == "acmebakery.com"

    def test_no_website_no_name(self):
        lead = {"website": None, "partner_name": ""}
        assert _infer_domain(lead) is None


class TestCompanyNameToDomain:
    def test_strips_inc(self):
        assert _company_name_to_domain("Acme Foods Inc") == "acmefoods.com"

    def test_strips_llc(self):
        assert _company_name_to_domain("Fresh Bakery LLC") == "freshbakery.com"

    def test_strips_co(self):
        assert _company_name_to_domain("Metro Parking Co") == "metroparking.com"

    def test_strips_corp(self):
        assert _company_name_to_domain("Global Packaging Corp") == "globalpackaging.com"

    def test_multi_word(self):
        assert _company_name_to_domain("Syracuse Parking Garage") == "syracuseparkinggarage.com"

    def test_too_short_returns_none(self):
        assert _company_name_to_domain("") is None
        assert _company_name_to_domain("AB") is None

    def test_lowercase(self):
        result = _company_name_to_domain("ACME FOODS INC")
        assert result == "acmefoods.com"


class TestHunterFinder:
    DOMAIN_SEARCH_RESPONSE = {
        "data": {
            "emails": [
                {
                    "value": "jane.doe@acmebakery.com",
                    "first_name": "Jane",
                    "last_name": "Doe",
                    "position": "Procurement Manager",
                    "linkedin": "https://linkedin.com/in/janedoe",
                    "confidence": 92,
                },
                {
                    "value": "bob.smith@acmebakery.com",
                    "first_name": "Bob",
                    "last_name": "Smith",
                    "position": "CEO",
                    "linkedin": None,
                    "confidence": 78,
                },
            ]
        }
    }

    def _finder(self):
        return HunterFinder(api_key="FAKE_KEY")

    def test_find_returns_candidates(self):
        finder = self._finder()
        mock_resp = MagicMock()
        mock_resp.json.return_value = self.DOMAIN_SEARCH_RESPONSE
        mock_resp.raise_for_status = MagicMock()

        with patch("modules.contact_discovery.finders.hunter.requests.get", return_value=mock_resp):
            lead = {"partner_name": "Acme Bakery", "website": "https://acmebakery.com", "id": 1}
            candidates = finder.find(lead, ["Procurement Manager"], {"services": ["hunter_io"]})

        assert len(candidates) == 2
        assert candidates[0].name == "Jane Doe"
        assert candidates[0].email == "jane.doe@acmebakery.com"
        assert candidates[0].title == "Procurement Manager"
        assert candidates[0].confidence == pytest.approx(0.92)
        assert candidates[0].source == "hunter_io"

    def test_no_api_key_returns_empty(self):
        finder = HunterFinder(api_key="")
        lead = {"partner_name": "Acme Bakery", "website": "https://acmebakery.com", "id": 1}
        result = finder.find(lead, ["Owner"], {"services": ["hunter_io"]})
        assert result == []

    def test_api_error_returns_empty(self):
        import requests as _requests
        finder = self._finder()
        with patch(
            "modules.contact_discovery.finders.hunter.requests.get",
            side_effect=_requests.RequestException("connection refused"),
        ):
            lead = {"partner_name": "Acme Bakery", "website": "https://acmebakery.com", "id": 1}
            result = finder.find(lead, ["Owner"], {"services": ["hunter_io"]})
        assert result == []

    def test_no_domain_returns_empty(self):
        finder = self._finder()
        lead = {"partner_name": "", "website": None, "id": 1}
        result = finder.find(lead, ["Owner"], {"services": ["hunter_io"]})
        assert result == []

    def test_non_hunter_service_skipped(self):
        finder = self._finder()
        lead = {"partner_name": "Acme", "website": "https://acme.com", "id": 1}
        result = finder.find(lead, ["Owner"], {"services": ["apollo_io"]})
        assert result == []

    def test_confidence_normalised_to_0_1(self):
        finder = self._finder()
        mock_resp = MagicMock()
        mock_resp.json.return_value = self.DOMAIN_SEARCH_RESPONSE
        mock_resp.raise_for_status = MagicMock()

        with patch("modules.contact_discovery.finders.hunter.requests.get", return_value=mock_resp):
            lead = {"partner_name": "Acme Bakery", "website": "https://acmebakery.com", "id": 1}
            candidates = finder.find(lead, ["Owner"], {"services": ["hunter_io"]})

        for c in candidates:
            assert 0.0 <= c.confidence <= 1.0


# ---------------------------------------------------------------------------
# LinkedIn queue
# ---------------------------------------------------------------------------

class TestLinkedInQueue:
    def _make_lead(self, lead_id, company, city="Syracuse", stream="stream_c"):
        return {
            "id": lead_id,
            "partner_name": company,
            "city": city,
            "state_id": [42, "New York"],
            "x_bd_stream": stream,
            "website": "",
        }

    def test_make_linkedin_search_url(self):
        url = make_linkedin_search_url("Acme Bakery", "Procurement Manager")
        assert "linkedin.com/search" in url
        assert "Acme" in url
        assert "Procurement" in url

    def test_write_queue_file_creates_csv(self):
        leads = [
            self._make_lead(1, "Acme Bakery"),
            self._make_lead(2, "Fresh Foods Co"),
        ]
        titles = {"stream_c": ["Procurement Manager", "Owner"]}

        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False, mode="w") as f:
            path = f.name
        try:
            written = write_queue_file(leads, titles, queue_file=path)
            assert written == 2

            with open(path, newline="") as f:
                rows = list(csv.DictReader(f))
            assert len(rows) == 2
            assert rows[0]["company_name"] == "Acme Bakery"
            assert rows[0]["status"] == "pending"
            assert rows[0]["priority_title"] == "Procurement Manager"
            assert "linkedin.com" in rows[0]["linkedin_search_url"]
        finally:
            os.unlink(path)

    def test_write_queue_file_no_duplicates(self):
        leads = [self._make_lead(1, "Acme Bakery")]
        titles = {"stream_c": ["Procurement Manager"]}

        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False, mode="w") as f:
            path = f.name
        try:
            write_queue_file(leads, titles, queue_file=path)
            # Run again — should not duplicate
            written2 = write_queue_file(leads, titles, queue_file=path)
            assert written2 == 0

            with open(path, newline="") as f:
                rows = list(csv.DictReader(f))
            assert len(rows) == 1
        finally:
            os.unlink(path)

    def test_format_queue_summary_no_file(self):
        result = format_queue_summary("/nonexistent/path/queue.csv")
        assert "No LinkedIn queue file" in result

    def test_format_queue_summary_with_data(self):
        leads = [
            self._make_lead(1, "Acme Bakery"),
            self._make_lead(2, "Fresh Foods"),
        ]
        titles = {"stream_c": ["Procurement Manager"]}

        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False, mode="w") as f:
            path = f.name
        try:
            write_queue_file(leads, titles, queue_file=path)
            summary = format_queue_summary(path)
            assert "Pending: 2" in summary
        finally:
            os.unlink(path)
