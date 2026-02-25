"""News search enrichment adapter.

Uses Google News RSS feed to find recent articles about a company, then uses
Claude Haiku to summarize and flag business-development signals.

Updates: description (appended with news summary)
"""

import re
import xml.etree.ElementTree as ET
from typing import Optional
from urllib.parse import quote_plus

import requests

from modules.lead_enrichment.adapters.base import BaseEnrichmentAdapter, EnrichmentResult
from shared.llm_client import HAIKU, LLMClient
from shared.logger import get_logger

logger = get_logger("lead_enrichment.news_search")

REQUEST_TIMEOUT = 10
MAX_ARTICLES = 3

GNEWS_RSS_URL = "https://news.google.com/rss/search"

SUMMARY_SYSTEM = """\
You summarize recent news about a company in 1-2 sentences for a business development team.
Highlight signals relevant to BD: expansion, new facilities, leadership changes, \
packaging/supply chain news, financial events, or growth announcements.
If there are no relevant or recent articles, respond with exactly: "No recent relevant news found."
Do NOT fabricate news. Only report what is explicitly stated in the provided articles."""


class NewsSearchAdapter(BaseEnrichmentAdapter):
    """Searches Google News RSS and summarizes results with Claude Haiku."""

    name = "news_search"

    def __init__(self, llm_client: Optional[LLMClient] = None):
        self._llm: Optional[LLMClient] = llm_client

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
        company = (lead.get("partner_name") or lead.get("name") or "").strip()
        if not company:
            return EnrichmentResult(
                source=self.name, success=False, error="No company name on lead"
            )

        if "description" not in fields_to_update:
            # Nothing to write — return success with no note
            return EnrichmentResult(source=self.name, success=True)

        logger.info("news_search: searching for '%s'", company)
        articles = _fetch_news(company)

        if not articles:
            logger.debug("news_search: no articles found for '%s'", company)
            return EnrichmentResult(source=self.name, success=True, description_note="")

        summary = self._summarize(company, articles)
        if not summary or "no recent relevant news" in summary.lower():
            return EnrichmentResult(source=self.name, success=True, description_note="")

        note = f"[News] {summary.strip()}"
        return EnrichmentResult(
            source=self.name,
            success=True,
            description_note=note,
        )

    def _summarize(self, company: str, articles: list[dict]) -> str:
        article_text = "\n".join(
            f"- {a['title']}: {a.get('snippet', '')}" for a in articles
        )
        prompt = f"Company: {company}\n\nRecent articles:\n{article_text}"
        try:
            return self._get_llm().complete(
                prompt=prompt,
                system=SUMMARY_SYSTEM,
                model=HAIKU,
                max_tokens=150,
            )
        except Exception as exc:
            logger.error("news_search: LLM failed for '%s': %s", company, exc)
            return ""


# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------

def _fetch_news(company: str) -> list[dict]:
    """Fetch top articles from Google News RSS for the company name."""
    query = quote_plus(f'"{company}"')
    url = f"{GNEWS_RSS_URL}?q={query}&hl=en-US&gl=US&ceid=US:en"
    try:
        resp = requests.get(
            url,
            timeout=REQUEST_TIMEOUT,
            headers={"User-Agent": "Mozilla/5.0"},
        )
        resp.raise_for_status()
        return _parse_rss(resp.text)[:MAX_ARTICLES]
    except requests.RequestException as exc:
        logger.debug("news_search: RSS fetch failed for '%s': %s", company, exc)
        return []


def _parse_rss(xml_text: str) -> list[dict]:
    """Parse Google News RSS XML into a list of {title, link, snippet} dicts."""
    articles = []
    try:
        root = ET.fromstring(xml_text)
        for item in root.findall(".//item"):
            title_el = item.find("title")
            desc_el = item.find("description")
            link_el = item.find("link")
            title = (title_el.text or "").strip() if title_el is not None else ""
            snippet = _strip_tags(desc_el.text or "") if desc_el is not None else ""
            link = (link_el.text or "").strip() if link_el is not None else ""
            if title:
                articles.append({"title": title, "snippet": snippet, "link": link})
    except ET.ParseError as exc:
        logger.debug("news_search: RSS parse error: %s", exc)
    return articles


def _strip_tags(html: str) -> str:
    """Remove HTML tags from a string."""
    return re.sub(r"<[^>]+>", "", html).strip()
