"""Trade data detail enrichment adapter.

Given a company name, looks up import history from a trade data service:
  - Whether the company imports from overseas
  - Supplier names and countries
  - Shipment volume

Updates: x_already_importing, x_current_supplier, x_import_source_country
"""

import re
import time
from typing import Optional
from urllib.parse import quote_plus

import requests
from bs4 import BeautifulSoup

from modules.lead_enrichment.adapters.base import BaseEnrichmentAdapter, EnrichmentResult
from shared.logger import get_logger

logger = get_logger("lead_enrichment.trade_data_detail")

BASE_URL = "https://www.your-trade-data-service.com"  # Replace with actual service URL
REQUEST_DELAY = 3.0
REQUEST_TIMEOUT = 15

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
}

# Two-letter country codes that indicate overseas (non-domestic) sourcing
OVERSEAS_COUNTRIES = {"CN", "TR", "TW", "VN", "TH", "IN", "DE", "IT", "KR", "MX", "PK", "BD"}

# Legal suffixes to strip when building URL slugs
_SUFFIX_RE = re.compile(
    r"\b(inc\.?|llc\.?|ltd\.?|corp\.?|co\.?|company|group|holdings)\b",
    flags=re.IGNORECASE,
)


class TradeDataDetailAdapter(BaseEnrichmentAdapter):
    """Searches a trade data service by company name to find import history."""

    name = "trade_data_detail"

    def __init__(self):
        self._session = requests.Session()
        self._session.headers.update(HEADERS)

    def enrich(
        self,
        lead: dict,
        fields_to_update: list[str],
        adapter_config: dict,
    ) -> EnrichmentResult:
        company = (lead.get("partner_name") or lead.get("name") or "").strip()
        if not company:
            return EnrichmentResult(
                source=self.name, success=False, error="No company name on lead"
            )

        logger.info("trade_data_detail: looking up '%s'", company)

        # Try direct company page first, then fall back to search
        slug = _company_to_slug(company)
        data = self._fetch_company_page(slug)
        if data is None:
            data = self._fetch_via_search(company)

        if not data:
            return EnrichmentResult(
                source=self.name,
                success=False,
                error=f"No trade data found for '{company}'",
            )

        odoo_fields: dict = {}
        note_parts: list[str] = []

        if "x_already_importing" in fields_to_update:
            odoo_fields["x_already_importing"] = True

        if "x_current_supplier" in fields_to_update and data.get("suppliers"):
            suppliers_str = "; ".join(data["suppliers"][:3])
            odoo_fields["x_current_supplier"] = suppliers_str
            note_parts.append(f"Suppliers: {suppliers_str}")

        if "x_import_source_country" in fields_to_update and data.get("countries"):
            country = data["countries"][0]
            odoo_fields["x_import_source_country"] = country
            note_parts.append(f"Import origin: {', '.join(data['countries'][:3])}")

        if data.get("shipment_count"):
            note_parts.append(f"Shipments: {data['shipment_count']}")

        note = f"[TradeData] {' | '.join(note_parts)}" if note_parts else ""

        return EnrichmentResult(
            source=self.name,
            success=True,
            fields_updated=odoo_fields,
            description_note=note,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _fetch_company_page(self, slug: str) -> Optional[dict]:
        url = f"{BASE_URL}/company/{slug}"
        try:
            time.sleep(REQUEST_DELAY)
            resp = self._session.get(url, timeout=REQUEST_TIMEOUT)
            if resp.status_code in (404, 410):
                return None
            resp.raise_for_status()
            return _parse_company_page(resp.text)
        except requests.RequestException as exc:
            logger.debug("trade_data_detail: fetch failed for %s: %s", url, exc)
            return None

    def _fetch_via_search(self, company: str) -> Optional[dict]:
        """Fall back to search if the direct URL didn't work."""
        encoded = quote_plus(company)
        url = f"{BASE_URL}/search?q={encoded}"
        try:
            time.sleep(REQUEST_DELAY)
            resp = self._session.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")
            link = soup.select_one("a[href*='/company/']")
            if not link:
                return None
            href = str(link.get("href", ""))
            if not href.startswith("http"):
                href = BASE_URL + href
            time.sleep(REQUEST_DELAY)
            resp2 = self._session.get(href, timeout=REQUEST_TIMEOUT)
            resp2.raise_for_status()
            return _parse_company_page(resp2.text)
        except requests.RequestException as exc:
            logger.debug("trade_data_detail: search failed for '%s': %s", company, exc)
            return None


# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------

def _company_to_slug(name: str) -> str:
    """Convert a company name to a URL slug.

    'ABC Bakery Inc' → 'abc-bakery'
    """
    cleaned = _SUFFIX_RE.sub("", name)
    slug = re.sub(r"[^a-z0-9]+", "-", cleaned.lower()).strip("-")
    return slug


def _parse_company_page(html: str) -> Optional[dict]:
    """Parse a trade data company page for supplier, country, and shipment data."""
    soup = BeautifulSoup(html, "html.parser")
    result: dict = {"suppliers": [], "countries": [], "shipment_count": None}

    text = soup.get_text(" ", strip=True)

    # Shipment count
    count_match = re.search(r"(\d[\d,]+)\s+shipments?", text, re.IGNORECASE)
    if count_match:
        result["shipment_count"] = count_match.group(1).replace(",", "")

    # Supplier names — try several CSS selector patterns
    for sel in ["[class*='supplier-name']", "[class*='supplierName']", "a[href*='/company/']"]:
        elems = soup.select(sel)
        names = [e.get_text(strip=True) for e in elems if e.get_text(strip=True)]
        if names:
            result["suppliers"] = list(dict.fromkeys(names))[:5]
            break

    # Overseas countries — scan for 2-letter codes from our known set
    code_matches = re.findall(r"\b([A-Z]{2})\b", text)
    seen = dict.fromkeys(code_matches)  # preserves order, removes dupes
    result["countries"] = [c for c in seen if c in OVERSEAS_COUNTRIES][:3]

    if not result["suppliers"] and not result["countries"] and not result["shipment_count"]:
        return None

    return result
