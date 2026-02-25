"""Google Maps Place Details enrichment adapter.

For a given lead, looks up the company in Google Places and fetches detail
information: rating, review count, business types.

Updates: description (appended with rating/reviews note), x_business_type
"""

import os
from typing import Optional

from modules.lead_enrichment.adapters.base import BaseEnrichmentAdapter, EnrichmentResult
from shared.logger import get_logger

logger = get_logger("lead_enrichment.google_maps_detail")

# Map from Google Places type strings → our x_business_type selection values
# Only map types we explicitly care about; everything else is left as-is.
TYPE_MAP: dict[str, str] = {
    "restaurant": "restaurant",
    "food": "food_manufacturer",
    "store": "retail",
    "parking": "parking",
    "real_estate_agency": "real_estate",
    "car_parking": "parking",
    "parking_lot": "parking",
    "parking_garage": "parking",
}


class GoogleMapsDetailAdapter(BaseEnrichmentAdapter):
    """Fetches Google Places details for a lead's company."""

    name = "google_maps_detail"

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
        company = (lead.get("partner_name") or lead.get("name") or "").strip()
        city = lead.get("city") or ""

        if not company:
            return EnrichmentResult(
                source=self.name, success=False, error="No company name on lead"
            )

        try:
            gmaps = self._get_gmaps()
        except (EnvironmentError, ImportError) as exc:
            return EnrichmentResult(source=self.name, success=False, error=str(exc))

        query = f"{company} {city}".strip()
        logger.info("google_maps_detail: searching for '%s'", query)

        try:
            search_result = gmaps.places(query=query)
        except Exception as exc:
            logger.error("google_maps_detail: places search failed: %s", exc)
            return EnrichmentResult(source=self.name, success=False, error=str(exc))

        candidates = search_result.get("results", [])
        if not candidates:
            return EnrichmentResult(
                source=self.name, success=False, error="No Google Maps results"
            )

        place_id = candidates[0].get("place_id")
        if not place_id:
            return EnrichmentResult(
                source=self.name, success=False, error="No place_id in search result"
            )

        try:
            details = gmaps.place(
                place_id=place_id,
                fields=["name", "rating", "user_ratings_total", "types", "formatted_address"],
            )
        except Exception as exc:
            logger.error(
                "google_maps_detail: place details failed for %s: %s", place_id, exc
            )
            return EnrichmentResult(source=self.name, success=False, error=str(exc))

        result_data = details.get("result", {})

        odoo_fields: dict = {}
        note_parts: list[str] = []

        types: list[str] = result_data.get("types", [])
        rating = result_data.get("rating")
        total_ratings = result_data.get("user_ratings_total")

        if "x_business_type" in fields_to_update:
            btype = _infer_business_type(types)
            if btype:
                odoo_fields["x_business_type"] = btype

        if rating is not None:
            count_str = f"{total_ratings} reviews" if total_ratings else "unknown reviews"
            note_parts.append(f"Google rating: {rating}/5 ({count_str})")

        note = ""
        if "description" in fields_to_update and note_parts:
            note = f"[Google Maps] {' | '.join(note_parts)}"

        return EnrichmentResult(
            source=self.name,
            success=True,
            fields_updated=odoo_fields,
            description_note=note,
        )


# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------

def _infer_business_type(types: list[str]) -> Optional[str]:
    """Map a Google Places type list to our x_business_type selection value."""
    for t in types:
        mapped = TYPE_MAP.get(t)
        if mapped:
            return mapped
    return None
