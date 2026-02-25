"""Trade data adapter — scrapes a trade data service for US importers by HS code.

This adapter has no official API; it scrapes public search results.
Be a good citizen:
  - 3-second delay between page fetches (REQUEST_DELAY)
  - Session-level URL cache (don't re-fetch during a single run)
  - Respects the exclude_suppliers_from filter (skip domestic-only shippers)

Selector robustness:
  The site's HTML structure may change.  The parser tries multiple
  strategies and falls back gracefully.  If parsing returns 0 results on a
  URL that should have results, check SELECTORS below and update to match
  the current HTML.  Running with LOG_FILE set will capture the raw HTML
  for inspection.
"""

import time
from typing import Optional
from urllib.parse import quote_plus

import requests
from bs4 import BeautifulSoup

from modules.prospect_research.adapters.base import BaseAdapter
from modules.prospect_research.normalizer import ProspectRecord
from shared.logger import get_logger

logger = get_logger("adapter.trade_data")

# ---------------------------------------------------------------------------
# Constants — update these to match your trade data service's HTML structure
# ---------------------------------------------------------------------------

BASE_URL = "https://www.your-trade-data-service.com"  # Replace with actual service URL

# URL templates for different search strategies
# Strategy 1: HS code product page
HS_CODE_URL = BASE_URL + "/hs-code/{hs_code}"
# Strategy 2: General search
SEARCH_URL = BASE_URL + "/search?q={query}"

REQUEST_DELAY = 3.0  # seconds between requests

# CSS selectors, tried in order until one returns results
# Outer: each company card element
CARD_SELECTORS = [
    "div.company-list-item",
    "div[class*='company-card']",
    "div[class*='CompanyCard']",
    "article[class*='company']",
    "li[class*='company']",
    "[data-company-name]",
]

# Within a card: name, location, supplier
NAME_SELECTORS = [
    "[data-company-name]",
    "h2", "h3",
    "[class*='company-name']",
    "[class*='CompanyName']",
    "a[href*='/company/']",
]

LOCATION_SELECTORS = [
    "[class*='location']",
    "[class*='city']",
    "[class*='address']",
    "[data-location]",
]

SUPPLIER_SELECTORS = [
    "[class*='supplier']",
    "[class*='origin']",
    "[class*='country']",
    "[data-supplier-countries]",
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}


class TradeDataAdapter(BaseAdapter):
    """Scrapes a trade data service for US companies importing under specified HS codes."""

    name = "trade_data"

    def __init__(self):
        self._session = requests.Session()
        self._session.headers.update(HEADERS)
        self._url_cache: dict[str, str] = {}  # url → response HTML

    def fetch(
        self,
        adapter_config: dict,
        stream: str,
        profile: dict,
    ) -> list[ProspectRecord]:
        """Fetch importers by HS code from the trade data service.

        Args:
            adapter_config: {hs_codes: [...], exclude_suppliers_from: [...]}
            stream:         BD stream key.
            profile:        Stream target profile (used for geography filter).
        """
        hs_codes: list[str] = adapter_config.get("hs_codes", [])
        exclude_from: list[str] = [
            c.upper() for c in adapter_config.get("exclude_suppliers_from", [])
        ]
        priority_states: list[str] = (
            profile.get("geography", {}).get("priority_states", [])
        )

        if not hs_codes:
            logger.warning("trade_data: no hs_codes configured — skipping")
            return []

        logger.info(
            "trade_data: fetching for %d HS code(s): %s",
            len(hs_codes),
            ", ".join(hs_codes),
        )

        all_records: list[ProspectRecord] = []
        seen_names: set[str] = set()

        for hs_code in hs_codes:
            records = self._fetch_by_hs_code(
                hs_code, stream, exclude_from, priority_states
            )
            for rec in records:
                key = rec.partner_name.lower().strip()
                if key not in seen_names:
                    seen_names.add(key)
                    all_records.append(rec)
                else:
                    logger.debug("trade_data: dedup within run — skipping '%s'", rec.partner_name)

        logger.info("trade_data: collected %d unique prospect(s)", len(all_records))
        return all_records

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _fetch_by_hs_code(
        self,
        hs_code: str,
        stream: str,
        exclude_from: list[str],
        priority_states: list[str],
    ) -> list[ProspectRecord]:
        """Fetch and parse one HS code page."""
        # Normalize HS code for URL: "3923.30" → "39233000" (pad to 8 digits)
        hs_normalized = hs_code.replace(".", "").replace(" ", "")
        if len(hs_normalized) == 6:
            hs_normalized += "00"

        url = HS_CODE_URL.format(hs_code=hs_normalized)
        logger.info("trade_data: GET %s", url)

        html = self._get_cached(url)
        if html is None:
            return []

        records = self._parse_company_cards(html, stream, exclude_from, priority_states, hs_code)

        if not records:
            # Fallback: try keyword search using the hs_code directly
            logger.debug(
                "trade_data: 0 results from HS code page, trying search fallback"
            )
            fallback_url = SEARCH_URL.format(query=quote_plus(hs_code))
            html2 = self._get_cached(fallback_url)
            if html2:
                records = self._parse_company_cards(
                    html2, stream, exclude_from, priority_states, hs_code
                )

        logger.info("trade_data: parsed %d record(s) for HS code %s", len(records), hs_code)
        return records

    def _get_cached(self, url: str) -> Optional[str]:
        """Fetch a URL with session caching and rate limiting."""
        if url in self._url_cache:
            logger.debug("trade_data: cache hit %s", url)
            return self._url_cache[url]

        try:
            time.sleep(REQUEST_DELAY)
            resp = self._session.get(url, timeout=15)
            resp.raise_for_status()
            html = resp.text
            self._url_cache[url] = html
            logger.debug(
                "trade_data: fetched %s (%d bytes)", url, len(html)
            )
            return html
        except requests.RequestException as exc:
            logger.error("trade_data: failed to fetch %s: %s", url, exc)
            return None

    def _parse_company_cards(
        self,
        html: str,
        stream: str,
        exclude_from: list[str],
        priority_states: list[str],
        hs_code: str,
    ) -> list[ProspectRecord]:
        """Parse company cards from a trade data results page."""
        soup = BeautifulSoup(html, "html.parser")
        cards = self._find_cards(soup)

        if not cards:
            logger.warning(
                "trade_data: no company cards found on page. "
                "HTML may be JS-rendered or selectors need updating. "
                "Set LOG_FILE env var and check the raw HTML."
            )
            return []

        logger.debug("trade_data: found %d card element(s)", len(cards))
        records = []

        for card in cards:
            rec = self._parse_single_card(card, stream, hs_code)
            if rec is None:
                continue

            # Apply exclude_from filter
            if exclude_from and rec.x_import_source_country:
                countries = [
                    c.strip().upper()
                    for c in rec.x_import_source_country.split(",")
                ]
                if all(c in exclude_from for c in countries if c):
                    logger.debug(
                        "trade_data: excluding '%s' (all suppliers in exclude list)",
                        rec.partner_name,
                    )
                    continue

            records.append(rec)

        return records

    def _find_cards(self, soup: BeautifulSoup) -> list:
        """Try each card selector until one returns results."""
        for selector in CARD_SELECTORS:
            cards = soup.select(selector)
            if cards:
                logger.debug("trade_data: matched card selector '%s'", selector)
                return cards
        return []

    def _extract_text(self, element, selectors: list[str]) -> Optional[str]:
        """Try each selector within an element; return first non-empty text."""
        for selector in selectors:
            try:
                found = element.select_one(selector)
                if found:
                    text = found.get_text(strip=True)
                    if text:
                        return text
                    # Also try data attribute
                    for attr in found.attrs:
                        if attr.startswith("data-") and found[attr]:
                            return str(found[attr]).strip()
            except Exception:
                continue
        return None

    def _parse_single_card(
        self, card, stream: str, hs_code: str
    ) -> Optional[ProspectRecord]:
        """Extract a ProspectRecord from a single company card element."""
        # Company name
        name = self._extract_text(card, NAME_SELECTORS)
        if not name:
            # Last resort: find first link text
            link = card.find("a")
            if link:
                name = link.get_text(strip=True)
        if not name:
            return None

        # Location (city, state)
        location_text = self._extract_text(card, LOCATION_SELECTORS)
        city, state_code = _parse_city_state(location_text)

        # Supplier country
        supplier_text = self._extract_text(card, SUPPLIER_SELECTORS)
        supplier_country = _normalize_country(supplier_text)

        # Build description
        desc_parts = []
        if supplier_text:
            desc_parts.append(f"Supplier countries: {supplier_text}")
        desc_parts.append(f"Source: trade_data HS {hs_code}")
        description = " | ".join(desc_parts) if desc_parts else None

        return ProspectRecord(
            partner_name=name,
            city=city,
            state_code=state_code,
            description=description,
            x_data_source="trade_data",
            x_bd_stream=stream,
            x_already_importing=True,
            x_import_source_country=supplier_country,
            raw={"html_text": card.get_text(separator=" ", strip=True)[:500]},
        )


# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------


def _parse_city_state(location_text: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    """Parse 'City, ST' or 'City ST' → (city, state_code)."""
    if not location_text:
        return None, None
    location_text = location_text.strip()
    # Match "City, ST" or "City ST" or just "City, State Name"
    import re
    m = re.match(r"^(.+?),?\s+([A-Z]{2})$", location_text)
    if m:
        return m.group(1).strip(), m.group(2).strip()
    return location_text, None


def _normalize_country(supplier_text: Optional[str]) -> Optional[str]:
    """Extract a country code or short name from supplier country text.

    The service might show "China (CN)" or just "China" or "CN".
    We store the raw text — enrichment module refines it.
    """
    if not supplier_text:
        return None
    # Extract ISO code if present in parentheses: "China (CN)" → "CN"
    import re
    m = re.search(r"\(([A-Z]{2})\)", supplier_text)
    if m:
        return m.group(1)
    # Return first 50 chars of raw text as-is
    return supplier_text[:50].strip()
