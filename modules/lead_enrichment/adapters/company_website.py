"""Company website enrichment adapter.

Scrapes homepage and /about page, then uses Claude Haiku to extract:
  - Company description / products summary  → appended to description
  - Approximate company size                → x_company_size
"""

import json
import re
import time
from typing import Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from modules.lead_enrichment.adapters.base import BaseEnrichmentAdapter, EnrichmentResult
from shared.llm_client import HAIKU, LLMClient
from shared.logger import get_logger

logger = get_logger("lead_enrichment.company_website")

REQUEST_DELAY = 1.5
REQUEST_TIMEOUT = 10
MAX_PAGE_CHARS = 4_000

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

EXTRACTION_SYSTEM = """\
You analyze company website text and extract key business information.
Return ONLY a JSON object with these keys (omit any you cannot determine with confidence):
{"description": "1-2 sentence summary of what the company makes/sells/does",
 "size": "small|medium|large|enterprise",
 "industry": "e.g. food manufacturing, commercial real estate"}
Size guide: small=<$5M, medium=$5M-$50M, large=$50M-$500M, enterprise=>$500M.
Return {} if no useful information is found. Do NOT fabricate details."""


class CompanyWebsiteEnrichmentAdapter(BaseEnrichmentAdapter):
    """Scrapes company website and uses Claude Haiku to extract enrichment data."""

    name = "company_website"

    def __init__(self, llm_client: Optional[LLMClient] = None):
        self._llm: Optional[LLMClient] = llm_client
        self._session = requests.Session()
        self._session.headers.update(HEADERS)

    def _get_llm(self) -> LLMClient:
        if self._llm is None:
            self._llm = LLMClient.from_env()
        return self._llm

    def enrich(
        self,
        lead: dict,
        fields_to_update: list[str],
        adapter_config: dict,
    ) -> EnrichmentResult:
        website = _normalize_url(lead.get("website") or "")
        company = lead.get("partner_name") or lead.get("name") or ""

        if not website:
            return EnrichmentResult(
                source=self.name,
                success=False,
                error="No website URL on lead",
            )

        logger.info("company_website: scraping %s for '%s'", website, company)

        # Fetch homepage and /about variants
        pages = ["", "/about", "/about-us"]
        texts: list[str] = []
        for path in pages:
            url = urljoin(website, path) if path else website
            text = self._fetch_text(url)
            if text:
                texts.append(text)
            time.sleep(REQUEST_DELAY)

        if not texts:
            return EnrichmentResult(
                source=self.name,
                success=False,
                error=f"No pages fetched from {website}",
            )

        combined = "\n\n".join(texts)[:MAX_PAGE_CHARS]
        extracted = self._extract_with_llm(combined, company)

        odoo_fields: dict = {}
        note = ""

        if "description" in fields_to_update and extracted.get("description"):
            note = f"[Website] {extracted['description']}"

        if "x_company_size" in fields_to_update and extracted.get("size"):
            odoo_fields["x_company_size"] = extracted["size"]

        return EnrichmentResult(
            source=self.name,
            success=True,
            fields_updated=odoo_fields,
            description_note=note,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _fetch_text(self, url: str) -> Optional[str]:
        """Fetch a URL and return stripped plain text."""
        try:
            resp = self._session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
            if resp.status_code in (404, 410):
                return None
            resp.raise_for_status()
            return _html_to_text(resp.text)[:MAX_PAGE_CHARS]
        except requests.exceptions.SSLError:
            if url.startswith("https://"):
                return self._fetch_text(url.replace("https://", "http://", 1))
            return None
        except requests.RequestException as exc:
            logger.debug("company_website: could not fetch %s: %s", url, exc)
            return None

    def _extract_with_llm(self, page_text: str, company_name: str) -> dict:
        """Call Claude Haiku to extract structured company info from page text."""
        prompt = f"Company: {company_name}\n\nWebsite text:\n{page_text}"
        try:
            raw = self._get_llm().complete(
                prompt=prompt,
                system=EXTRACTION_SYSTEM,
                model=HAIKU,
                max_tokens=256,
            )
        except Exception as exc:
            logger.error(
                "company_website: LLM extraction failed for '%s': %s", company_name, exc
            )
            return {}
        return _parse_json_object(raw)


# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------

def _normalize_url(url: str) -> str:
    """Ensure URL has a scheme and no trailing slash."""
    url = url.strip()
    if not url:
        return ""
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    return url.rstrip("/")


def _html_to_text(html: str) -> str:
    """Strip HTML tags and collapse whitespace to clean plain text."""
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "nav", "footer", "head", "noscript"]):
        tag.decompose()
    text = soup.get_text(separator=" ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _parse_json_object(raw: str) -> dict:
    """Extract the first JSON object from a string (LLM may add prose)."""
    m = re.search(r"\{.*?\}", raw, re.DOTALL)
    if not m:
        return {}
    try:
        return json.loads(m.group(0))
    except json.JSONDecodeError:
        return {}
