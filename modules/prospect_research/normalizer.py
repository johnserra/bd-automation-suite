"""Normalized prospect record for the Prospect Research module.

All adapters return a list of ProspectRecord objects.  The orchestrator
converts these to Odoo lead creation dicts via to_odoo_values().
"""

import re
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class ProspectRecord:
    """Normalized company/location record returned by any adapter.

    Fields that are None are omitted from the Odoo write dict, so existing
    field values are never overwritten with nulls.
    """

    # ---- Required ----
    partner_name: str

    # ---- Standard Odoo address fields ----
    street: Optional[str] = None
    city: Optional[str] = None
    state_code: Optional[str] = None   # Two-letter code, e.g. "NY"
    zip: Optional[str] = None
    country_code: str = "US"

    # ---- Contact ----
    phone: Optional[str] = None
    website: Optional[str] = None

    # ---- Description / notes ----
    description: Optional[str] = None

    # ---- BD custom fields ----
    x_data_source: Optional[str] = None
    x_bd_stream: Optional[str] = None
    x_enrichment_status: str = "pending"

    # ---- Stream C (sourcing) specific ----
    x_already_importing: Optional[bool] = None
    x_import_source_country: Optional[str] = None  # ISO country code, e.g. "CN"
    x_current_supplier: Optional[str] = None

    # ---- Stream A (acquisition) specific ----
    x_property_type: Optional[str] = None
    x_estimated_spaces: Optional[int] = None

    # ---- Metadata (not stored in Odoo) ----
    place_id: Optional[str] = None          # Google Maps place ID
    rating: Optional[float] = None          # Google Maps rating
    raw: dict = field(default_factory=dict) # Raw source data for debugging

    def to_odoo_values(
        self,
        stream: str,
        stage_id: int,
        state_id: Optional[int] = None,
        country_id: Optional[int] = None,
    ) -> dict:
        """Build a dict suitable for OdooClient.create_lead().

        Only non-None values are included, so the Odoo defaults are
        preserved for fields we have no data for.

        Args:
            stream:     BD stream key, e.g. 'stream_a'.
            stage_id:   Odoo crm.stage ID for "Research".
            state_id:   Odoo res.country.state ID (resolved from state_code).
            country_id: Odoo res.country ID for US (resolved externally).
        """
        name = _make_lead_title(self.partner_name, stream)

        values: dict = {
            "name": name,
            "partner_name": self.partner_name,
            "stage_id": stage_id,
            "x_bd_stream": stream,
            "x_enrichment_status": self.x_enrichment_status,
        }

        # Address
        if self.street:
            values["street"] = self.street
        if self.city:
            values["city"] = self.city
        if state_id:
            values["state_id"] = state_id
        if self.zip:
            values["zip"] = self.zip
        if country_id:
            values["country_id"] = country_id

        # Contact
        if self.phone:
            values["phone"] = self.phone
        if self.website:
            values["website"] = self.website

        # Description
        if self.description:
            values["description"] = self.description

        # Custom fields
        if self.x_data_source:
            values["x_data_source"] = self.x_data_source
        if self.x_already_importing is not None:
            values["x_already_importing"] = self.x_already_importing
        if self.x_import_source_country:
            values["x_import_source_country"] = self.x_import_source_country
        if self.x_current_supplier:
            values["x_current_supplier"] = self.x_current_supplier
        if self.x_property_type:
            values["x_property_type"] = self.x_property_type
        if self.x_estimated_spaces is not None:
            values["x_estimated_spaces"] = self.x_estimated_spaces

        return values


def _make_lead_title(partner_name: str, stream: str) -> str:
    """Format the Odoo lead name field.

    Example: "Acme Foods Inc — Stream C"
    """
    stream_label = stream.replace("_", " ").title()
    return f"{partner_name} — {stream_label}"


def parse_google_address(formatted_address: str) -> dict:
    """Parse a Google Maps formatted_address string into components.

    Handles formats like:
        "123 Main St, Syracuse, NY 13202, USA"
        "456 Park Ave, New York, NY 10022, USA"
        "789 Oak Rd, Buffalo, NY 14201, USA"
        "Some Parking Lot, Rochester, NY, USA"  (no zip)

    Returns dict with keys: street, city, state_code, zip (all may be None).
    """
    result = {"street": None, "city": None, "state_code": None, "zip": None}

    if not formatted_address:
        return result

    # Strip trailing country
    address = re.sub(r",?\s*(USA|United States|US)\s*$", "", formatted_address).strip()

    parts = [p.strip() for p in address.split(",")]

    if len(parts) >= 3:
        # "street, city, STATE ZIP"  or  "street, city, STATE"
        result["street"] = parts[0]
        result["city"] = parts[1]
        state_zip = parts[2].strip()
    elif len(parts) == 2:
        # "city, STATE ZIP"
        result["city"] = parts[0]
        state_zip = parts[1].strip()
    elif len(parts) == 1:
        result["city"] = parts[0]
        return result
    else:
        return result

    # Parse "NY 13202" or just "NY"
    m = re.match(r"([A-Z]{2})\s*(\d{5}(?:-\d{4})?)?", state_zip)
    if m:
        result["state_code"] = m.group(1)
        result["zip"] = m.group(2)

    return result
