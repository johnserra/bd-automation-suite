"""Google Maps (Places API) adapter for Prospect Research.

Uses the Text Search endpoint to find locations matching query templates.
Each search_query in the YAML config is expanded with each city, then
submitted to the Places API.

API cost: ~$0.032 per text search request (200 results credit/month free).
The adapter caps results per query to avoid runaway costs.

Requires:
    GOOGLE_MAPS_API_KEY environment variable.

Configuration example (stream_a.yaml):
    data_sources:
      google_maps:
        enabled: true
        search_queries:
          - "search term {city}"
          - "keyword {city}"
        fetch_details: false   # Set true to get phone numbers (extra API calls)
        max_results_per_query: 20
"""

import os
import re
import time
from typing import Optional

import googlemaps

from modules.prospect_research.adapters.base import BaseAdapter
from modules.prospect_research.normalizer import ProspectRecord, parse_google_address
from shared.logger import get_logger

logger = get_logger("adapter.google_maps")

REQUEST_DELAY = 0.5   # seconds between API calls (well within rate limits)
DEFAULT_MAX_RESULTS = 20


class GoogleMapsAdapter(BaseAdapter):
    """Searches Google Places API for locations matching query templates."""

    name = "google_maps"

    def __init__(self, api_key: Optional[str] = None):
        self._api_key = api_key or os.getenv("GOOGLE_MAPS_API_KEY", "").strip()
        self._client: Optional[googlemaps.Client] = None

    def _get_client(self) -> googlemaps.Client:
        if self._client is None:
            if not self._api_key:
                raise EnvironmentError(
                    "GOOGLE_MAPS_API_KEY is not set. "
                    "Add it to .env or export it before running."
                )
            self._client = googlemaps.Client(key=self._api_key)
        return self._client

    def fetch(
        self,
        adapter_config: dict,
        stream: str,
        profile: dict,
    ) -> list[ProspectRecord]:
        """Run each query × city combination through Places text search.

        Args:
            adapter_config: {search_queries: [...], fetch_details: bool, max_results_per_query: int}
            stream:         BD stream key.
            profile:        Stream target profile.
        """
        queries: list[str] = adapter_config.get("search_queries", [])
        fetch_details: bool = adapter_config.get("fetch_details", False)
        max_per_query: int = adapter_config.get("max_results_per_query", DEFAULT_MAX_RESULTS)
        exclude_operators: list[str] = [
            op.lower() for op in profile.get("exclude_operators", [])
        ]

        # Gather cities from profile geography
        cities: list[str] = profile.get("geography", {}).get("cities", [])
        if not cities:
            cities = [""]   # Run without city substitution

        if not queries:
            logger.warning("GoogleMaps: no search_queries configured — skipping")
            return []

        client = self._get_client()

        all_records: list[ProspectRecord] = []
        seen_place_ids: set[str] = set()

        for query_template in queries:
            for city in cities:
                query = query_template.format(city=city).strip()
                logger.info("GoogleMaps: searching '%s'", query)
                records = self._search_text(
                    client, query, stream, fetch_details, max_per_query
                )
                for rec in records:
                    # Deduplicate by place_id within this run
                    pid = rec.place_id or rec.partner_name.lower()
                    if pid in seen_place_ids:
                        continue
                    seen_place_ids.add(pid)

                    # Apply exclude_operators filter
                    if exclude_operators:
                        name_lower = rec.partner_name.lower()
                        if any(op in name_lower for op in exclude_operators):
                            logger.debug(
                                "GoogleMaps: excluding '%s' (matches exclude_operators)",
                                rec.partner_name,
                            )
                            continue

                    all_records.append(rec)
                time.sleep(REQUEST_DELAY)

        logger.info("GoogleMaps: collected %d unique result(s)", len(all_records))
        return all_records

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _search_text(
        self,
        client: googlemaps.Client,
        query: str,
        stream: str,
        fetch_details: bool,
        max_results: int,
    ) -> list[ProspectRecord]:
        """Execute a single Places text search and return ProspectRecords."""
        records = []
        page_token = None
        fetched = 0

        while fetched < max_results:
            try:
                kwargs = {"query": query, "language": "en"}
                if page_token:
                    kwargs["page_token"] = page_token
                    time.sleep(2)  # Google requires a short delay before using next_page_token

                response = client.places(**kwargs)
            except Exception as exc:
                logger.error("GoogleMaps: API error for query '%s': %s", query, exc)
                break

            results = response.get("results", [])
            if not results:
                break

            for place in results:
                if fetched >= max_results:
                    break
                rec = self._place_to_record(place, stream)
                if rec:
                    if fetch_details and rec.place_id:
                        rec = self._enrich_with_details(client, rec)
                    records.append(rec)
                    fetched += 1

            page_token = response.get("next_page_token")
            if not page_token:
                break

        logger.debug("GoogleMaps: query '%s' → %d result(s)", query, len(records))
        return records

    def _place_to_record(self, place: dict, stream: str) -> Optional[ProspectRecord]:
        """Convert a single Places API result dict to a ProspectRecord."""
        name = place.get("name", "").strip()
        if not name:
            return None

        formatted_address = place.get("formatted_address", "")
        addr = parse_google_address(formatted_address)

        place_id = place.get("place_id")
        rating = place.get("rating")
        types = place.get("types", [])

        # Build a light description
        desc_parts = []
        if rating:
            desc_parts.append(f"Google rating: {rating}/5")
        if types:
            readable_types = [t.replace("_", " ") for t in types[:3]]
            desc_parts.append(f"Types: {', '.join(readable_types)}")
        if formatted_address:
            desc_parts.append(f"Address: {formatted_address}")

        return ProspectRecord(
            partner_name=name,
            street=addr["street"],
            city=addr["city"],
            state_code=addr["state_code"],
            zip=addr["zip"],
            description="\n".join(desc_parts) if desc_parts else None,
            x_data_source="google_maps",
            x_bd_stream=stream,
            place_id=place_id,
            rating=rating,
            raw=place,
        )

    def _enrich_with_details(
        self, client: googlemaps.Client, rec: ProspectRecord
    ) -> ProspectRecord:
        """Fetch place details to add phone number and website."""
        try:
            time.sleep(REQUEST_DELAY)
            detail = client.place(
                rec.place_id,
                fields=["formatted_phone_number", "website"],
            )
            result = detail.get("result", {})
            rec.phone = result.get("formatted_phone_number")
            rec.website = result.get("website")
        except Exception as exc:
            logger.warning(
                "GoogleMaps: detail fetch failed for '%s': %s",
                rec.partner_name,
                exc,
            )
        return rec
