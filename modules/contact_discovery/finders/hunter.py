"""Hunter.io email finder adapter.

Uses two Hunter.io endpoints:
  1. domain-search  — returns all known emails at a domain (with names/titles).
                      Good first step: cheap, gets all contacts at once.
  2. email-finder   — finds a specific person's email given name + domain.
                      Used when we know the name from another source.

Free tier: 25 searches/month (domain-search counts as 1, email-finder as 1).
Requires: HUNTER_IO_API_KEY environment variable.

API docs: https://hunter.io/api-documentation
"""

import os
import re
from typing import Optional
from urllib.parse import urlparse

import requests

from modules.contact_discovery.finders.base import BaseContactFinder, ContactCandidate
from shared.logger import get_logger

logger = get_logger("contact_discovery.hunter")

HUNTER_BASE = "https://api.hunter.io/v2"
REQUEST_TIMEOUT = 10


class HunterFinder(BaseContactFinder):
    """Finds email addresses via Hunter.io API."""

    name = "email_finder"

    def __init__(self, api_key: Optional[str] = None):
        self._api_key = api_key or os.getenv("HUNTER_IO_API_KEY", "").strip()

    def find(
        self,
        lead: dict,
        priority_titles: list[str],
        finder_config: dict,
    ) -> list[ContactCandidate]:
        if not self._api_key:
            logger.warning(
                "Hunter.io: HUNTER_IO_API_KEY not set — skipping email finder"
            )
            return []

        # Only "hunter_io" is implemented; other services would be separate adapters
        services: list[str] = finder_config.get("services", ["hunter_io"])
        if "hunter_io" not in services:
            return []

        domain = _infer_domain(lead)
        if not domain:
            logger.debug(
                "Hunter.io: could not determine domain for '%s' — skipping",
                lead.get("partner_name", "?"),
            )
            return []

        company_name = lead.get("partner_name") or ""
        logger.info("Hunter.io: domain-search for %s ('%s')", domain, company_name)

        candidates = self._domain_search(domain, priority_titles)
        logger.info(
            "Hunter.io: found %d candidate(s) at %s", len(candidates), domain
        )
        return candidates

    # ------------------------------------------------------------------
    # API calls
    # ------------------------------------------------------------------

    def _domain_search(
        self,
        domain: str,
        priority_titles: list[str],
    ) -> list[ContactCandidate]:
        """Search all known emails at a domain, filter by relevant titles."""
        try:
            resp = requests.get(
                f"{HUNTER_BASE}/domain-search",
                params={"domain": domain, "api_key": self._api_key, "limit": 20},
                timeout=REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as exc:
            logger.error("Hunter.io: domain-search failed for %s: %s", domain, exc)
            return []

        emails = data.get("data", {}).get("emails", [])
        if not emails:
            logger.debug("Hunter.io: no emails returned for %s", domain)
            return []

        candidates = []
        for entry in emails:
            name = _build_name(entry.get("first_name"), entry.get("last_name"))
            if not name:
                continue
            title = (entry.get("position") or "").strip() or None
            email = (entry.get("value") or "").strip() or None
            li_url = (entry.get("linkedin") or "").strip() or None
            confidence_pct = entry.get("confidence", 0)

            candidates.append(ContactCandidate(
                name=name,
                title=title,
                email=email,
                linkedin_url=li_url,
                confidence=confidence_pct / 100.0,
                source="hunter_io",
                raw=entry,
            ))

        return candidates

    def email_finder(
        self, domain: str, first_name: str, last_name: str
    ) -> Optional[ContactCandidate]:
        """Find a specific person's email.  Returns None if not found."""
        try:
            resp = requests.get(
                f"{HUNTER_BASE}/email-finder",
                params={
                    "domain": domain,
                    "first_name": first_name,
                    "last_name": last_name,
                    "api_key": self._api_key,
                },
                timeout=REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            data = resp.json().get("data", {})
        except requests.RequestException as exc:
            logger.error(
                "Hunter.io: email-finder failed for %s %s @ %s: %s",
                first_name, last_name, domain, exc,
            )
            return None

        email = data.get("email")
        if not email:
            return None

        name = _build_name(first_name, last_name)
        return ContactCandidate(
            name=name,
            email=email,
            confidence=data.get("score", 0) / 100.0,
            source="hunter_io",
            raw=data,
        )


# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------

def _infer_domain(lead: dict) -> Optional[str]:
    """Extract or guess the company domain for Hunter.io queries.

    Priority:
    1. Parse domain from lead's website field.
    2. Heuristic from company name (best-effort, not guaranteed).
    """
    website = (lead.get("website") or "").strip()
    if website:
        if not website.startswith(("http://", "https://")):
            website = "https://" + website
        try:
            parsed = urlparse(website)
            host = parsed.netloc.lower().removeprefix("www.")
            if host and "." in host:
                return host
        except Exception:
            pass

    # Heuristic: slug company name → company.com
    company = (lead.get("partner_name") or "").strip()
    return _company_name_to_domain(company)


def _company_name_to_domain(name: str) -> Optional[str]:
    """Best-effort domain inference from company name.

    "Acme Bakery Inc" → "acmebakery.com"
    "Fresh Foods Co."  → "freshfoods.com"

    Returns None if the name is too short or ambiguous to guess from.
    """
    if not name:
        return None

    # Strip common legal suffixes
    cleaned = re.sub(
        r"\b(inc\.?|llc\.?|ltd\.?|corp\.?|co\.?|company|group|holdings|international|"
        r"enterprises?|solutions?|services?|industries|associates?)\b",
        "",
        name,
        flags=re.IGNORECASE,
    )
    # Keep only alphanumeric
    slug = re.sub(r"[^a-z0-9]", "", cleaned.lower())
    if len(slug) < 3:
        return None
    return f"{slug}.com"


def _build_name(first: Optional[str], last: Optional[str]) -> str:
    parts = [p.strip() for p in [first, last] if p and p.strip()]
    return " ".join(parts)
