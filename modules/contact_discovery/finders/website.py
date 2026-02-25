"""Company website contact finder.

Fetches /about, /team, /leadership, /contact pages from the company's
website (if available on the Odoo lead), strips HTML to clean text, then
calls Claude Haiku to extract structured contact information.

LLM cost estimate: ~$0.001–0.003 per company (Haiku, ~2K tokens).
"""

import json
import re
import time
from typing import Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from modules.contact_discovery.finders.base import BaseContactFinder, ContactCandidate
from shared.llm_client import LLMClient, HAIKU
from shared.logger import get_logger

logger = get_logger("contact_discovery.website")

REQUEST_DELAY = 1.5   # seconds between page fetches
REQUEST_TIMEOUT = 10

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

# Max characters of page text to send to LLM (keep costs low)
MAX_PAGE_CHARS = 3_000

EXTRACTION_SYSTEM = """\
You extract contact information from company webpages. Return ONLY valid JSON, \
no explanation. Format: [{"name":"...","title":"...","email":"...","linkedin":"..."}]. \
Omit any key that is not present. Include only people with leadership/management titles. \
Return an empty list [] if no relevant contacts are found."""


class WebsiteContactFinder(BaseContactFinder):
    """Scrapes company pages and uses Claude Haiku to extract contacts."""

    name = "company_website"

    def __init__(self, llm_client: Optional[LLMClient] = None):
        self._llm: Optional[LLMClient] = llm_client
        self._session = requests.Session()
        self._session.headers.update(HEADERS)
        self._page_cache: dict[str, str] = {}   # url → stripped text

    def _get_llm(self) -> LLMClient:
        if self._llm is None:
            self._llm = LLMClient.from_env()
        return self._llm

    def find(
        self,
        lead: dict,
        priority_titles: list[str],
        finder_config: dict,
    ) -> list[ContactCandidate]:
        website = _normalize_url(lead.get("website") or "")
        if not website:
            logger.debug(
                "Website: no website URL for '%s' — skipping",
                lead.get("partner_name", "?"),
            )
            return []

        pages_to_check: list[str] = finder_config.get(
            "pages_to_check", ["/about", "/team", "/leadership", "/contact"]
        )
        company_name = lead.get("partner_name") or ""
        logger.info("Website: checking %s for '%s'", website, company_name)

        # Fetch each page and collect text
        all_text_parts: list[str] = []
        for path in pages_to_check:
            url = urljoin(website, path)
            text = self._fetch_page_text(url)
            if text:
                all_text_parts.append(f"[PAGE: {path}]\n{text}")
            time.sleep(REQUEST_DELAY)

        if not all_text_parts:
            logger.debug("Website: no pages fetched for '%s'", company_name)
            return []

        combined = "\n\n".join(all_text_parts)[:MAX_PAGE_CHARS * len(pages_to_check)]

        candidates = self._extract_with_llm(combined, company_name, priority_titles)
        logger.info(
            "Website: found %d candidate(s) for '%s'", len(candidates), company_name
        )
        return candidates

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _fetch_page_text(self, url: str) -> Optional[str]:
        """Fetch a URL and return stripped plain text, using cache."""
        if url in self._page_cache:
            return self._page_cache[url]

        try:
            resp = self._session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
            # Treat 404/410 as "page doesn't exist" — not an error
            if resp.status_code in (404, 410):
                self._page_cache[url] = ""
                return None
            resp.raise_for_status()
            text = _html_to_text(resp.text)[:MAX_PAGE_CHARS]
            self._page_cache[url] = text
            logger.debug("Website: fetched %s (%d chars)", url, len(text))
            return text
        except requests.exceptions.SSLError:
            # Try http fallback
            if url.startswith("https://"):
                return self._fetch_page_text(url.replace("https://", "http://", 1))
            logger.debug("Website: SSL error on %s", url)
            return None
        except requests.RequestException as exc:
            logger.debug("Website: could not fetch %s: %s", url, exc)
            self._page_cache[url] = ""
            return None

    def _extract_with_llm(
        self,
        page_text: str,
        company_name: str,
        priority_titles: list[str],
    ) -> list[ContactCandidate]:
        """Call Claude Haiku to extract structured contacts from page text."""
        title_hint = ", ".join(priority_titles[:5]) if priority_titles else "leadership"
        prompt = (
            f"Company: {company_name}\n"
            f"Looking for: {title_hint}\n\n"
            f"Webpage text:\n{page_text}"
        )

        try:
            raw = self._get_llm().complete(
                prompt=prompt,
                system=EXTRACTION_SYSTEM,
                model=HAIKU,
                max_tokens=512,
            )
        except Exception as exc:
            logger.error("Website: LLM extraction failed for '%s': %s", company_name, exc)
            return []

        return _parse_llm_response(raw, source="website")


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
    # Remove scripts, styles, nav, footer (noise)
    for tag in soup(["script", "style", "nav", "footer", "head", "noscript"]):
        tag.decompose()
    text = soup.get_text(separator=" ")
    # Collapse runs of whitespace/newlines
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _parse_llm_response(raw: str, source: str) -> list[ContactCandidate]:
    """Parse a JSON list from the LLM response into ContactCandidate objects."""
    # Extract JSON array from response (LLM may add prose around it)
    m = re.search(r"\[.*?\]", raw, re.DOTALL)
    if not m:
        return []

    try:
        items = json.loads(m.group(0))
    except json.JSONDecodeError:
        return []

    candidates = []
    for item in items:
        name = (item.get("name") or "").strip()
        if not name:
            continue
        candidates.append(ContactCandidate(
            name=name,
            title=(item.get("title") or "").strip() or None,
            email=(item.get("email") or "").strip() or None,
            linkedin_url=(item.get("linkedin") or "").strip() or None,
            confidence=0.7,
            source=source,
            raw=item,
        ))
    return candidates
