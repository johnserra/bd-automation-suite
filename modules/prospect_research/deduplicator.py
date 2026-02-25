"""Deduplication logic for Prospect Research.

Before creating a lead in Odoo, we check whether a matching company already
exists.  The match fields are configurable per stream in the YAML config
(dedup.match_on).

Supported match fields:
    partner_name — fuzzy-matched via thefuzz (threshold 85)
    city         — exact match (case-insensitive)
    street       — exact match (case-insensitive) — for parking where two
                   different operators may manage adjacent lots
"""

from modules.prospect_research.normalizer import ProspectRecord
from shared.logger import get_logger
from shared.odoo_client import OdooClient

logger = get_logger("prospect_research.dedup")


def is_duplicate(
    record: ProspectRecord,
    odoo: OdooClient,
    match_on: list[str],
) -> bool:
    """Return True if a sufficiently similar lead already exists in Odoo.

    Always fuzzy-matches on partner_name.  Additional fields in match_on
    are checked as exact constraints (all must match for a hit to count).

    Args:
        record:   The candidate prospect.
        odoo:     Live OdooClient instance.
        match_on: List of field names from the YAML dedup.match_on config.
                  Supported values: partner_name, city, street.
    """
    if not record.partner_name or not record.partner_name.strip():
        logger.warning("Dedup: skipping record with no partner_name")
        return False

    # city is used as a secondary narrowing filter for the Odoo query
    city = record.city if "city" in match_on else None

    matches = odoo.search_duplicate(record.partner_name, city=city)

    if not matches:
        return False

    # If match_on also includes 'street', further narrow: require street match
    if "street" in match_on and record.street:
        street_lower = record.street.lower().strip()
        matches = [
            m for m in matches
            if (m.get("street") or "").lower().strip() == street_lower
        ]

    if matches:
        logger.info(
            "Dedup: '%s' in %s — matches existing lead(s): %s",
            record.partner_name,
            record.city or "?",
            [m["partner_name"] for m in matches[:3]],
        )
        return True

    return False


def split_new_and_duplicate(
    records: list[ProspectRecord],
    odoo: OdooClient,
    match_on: list[str],
) -> tuple[list[ProspectRecord], list[ProspectRecord]]:
    """Partition records into (new, duplicate) lists.

    Args:
        records:  All prospect records from all adapters.
        odoo:     Live OdooClient instance.
        match_on: Field list from YAML dedup config.

    Returns:
        (new_records, duplicate_records)
    """
    new_records: list[ProspectRecord] = []
    duplicate_records: list[ProspectRecord] = []

    for rec in records:
        if is_duplicate(rec, odoo, match_on):
            duplicate_records.append(rec)
        else:
            new_records.append(rec)

    logger.info(
        "Dedup complete: %d new, %d duplicate(s)",
        len(new_records),
        len(duplicate_records),
    )
    return new_records, duplicate_records
