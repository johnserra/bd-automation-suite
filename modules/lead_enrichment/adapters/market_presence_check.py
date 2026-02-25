"""Market presence check enrichment adapter.

Searches Google Maps for known operators near the lead's address to determine
if there is already a known operator managing the property.

Updates: x_current_operator

Configure known_operators in enrichment.yaml (under the adapter's source config),
or rely on the DEFAULT_KNOWN_OPERATORS list as a starting point.
"""

import os
from typing import Optional

from modules.lead_enrichment.adapters.base import BaseEnrichmentAdapter, EnrichmentResult
from shared.logger import get_logger

logger = get_logger("lead_enrichment.market_presence_check")

# Default list of known operators to watch for — replace with operators
# relevant to your market before deploying.
DEFAULT_KNOWN_OPERATORS = [
    "Operator A",
    "Operator B",
    "Operator C",
    "Operator D",
    "Operator E",
]


class MarketPresenceCheckAdapter(BaseEnrichmentAdapter):
    """Searches Google Maps for known operators near the lead's address."""

    name = "market_presence_check"

    def __init__(self, gmaps_client=None):
        # Injected in tests; lazily constructed from env in production
        self._gmaps = gmaps_client

    def _get_gmaps(self):
        if self._gmaps is not None:
            return self._gmaps
        api_key = os.getenv("GOOGLE_MAPS_API_KEY", "").strip()
        if not api_key:
            raise EnvironmentError("GOOGLE_MAPS_API_KEY not set")
        try:
            import googlemaps  # noqa: PLC0415
        except ImportError as exc:
            raise ImportError("googlemaps package not installed") from exc
        self._gmaps = googlemaps.Client(key=api_key)
        return self._gmaps

    def enrich(
        self,
        lead: dict,
        fields_to_update: list[str],
        adapter_config: dict,
    ) -> EnrichmentResult:
        if "x_current_operator" not in fields_to_update:
            # Nothing to write — return success with no changes
            return EnrichmentResult(source=self.name, success=True)

        address_parts = [
            lead.get("street") or "",
            lead.get("city") or "",
        ]
        company = (lead.get("partner_name") or "").strip()
        location_query = " ".join(p for p in address_parts if p).strip() or company

        if not location_query:
            return EnrichmentResult(
                source=self.name, success=False, error="No address or company name"
            )

        known_operators: list[str] = adapter_config.get(
            "known_operators", DEFAULT_KNOWN_OPERATORS
        )

        # search_term is configurable — set it in enrichment.yaml under this adapter's config
        search_term = adapter_config.get("search_term", "")
        query = f"{search_term} {location_query}".strip() if search_term else location_query

        try:
            gmaps = self._get_gmaps()
        except (EnvironmentError, ImportError) as exc:
            return EnrichmentResult(source=self.name, success=False, error=str(exc))

        logger.info("market_presence_check: searching '%s'", query)

        try:
            search = gmaps.places(query=query)
        except Exception as exc:
            logger.error("market_presence_check: search failed: %s", exc)
            return EnrichmentResult(source=self.name, success=False, error=str(exc))

        results = search.get("results", [])
        operator = _find_matching_operator(results, known_operators)

        odoo_fields: dict = {}
        note = ""
        if operator:
            odoo_fields["x_current_operator"] = operator
            note = f"[MarketPresence] Current operator: {operator}"
            logger.info(
                "market_presence_check: found operator '%s' for '%s'",
                operator,
                company or location_query,
            )

        return EnrichmentResult(
            source=self.name,
            success=True,
            fields_updated=odoo_fields,
            description_note=note,
        )


# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------

def _find_matching_operator(results: list[dict], known_operators: list[str]) -> Optional[str]:
    """Return the first result name that matches a known operator (case-insensitive)."""
    for result in results:
        name = result.get("name", "")
        for op in known_operators:
            if op.lower() in name.lower() or name.lower() in op.lower():
                return name
    return None
