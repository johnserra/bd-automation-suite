"""LinkedIn manual-work queue generator.

For leads where automated finders couldn't locate a contact, this module
generates LinkedIn people-search URLs and writes a queue file for manual
review.

LinkedIn scraping is NOT implemented here (fragile, against ToS).  Instead,
John opens the generated URLs, finds the right person, and pastes the result
back.  The queue file tracks status so completed entries aren't repeated.

Queue file format (CSV):
  lead_id, company_name, city, state, stream, website, priority_title,
  linkedin_url, status, notes

Status values:
  pending  — needs manual lookup
  found    — contact info added to Odoo manually
  skipped  — intentionally skipped (not a fit)
"""

import csv
import os
from datetime import date
from pathlib import Path
from typing import Optional
from urllib.parse import quote_plus

from shared.logger import get_logger

logger = get_logger("contact_discovery.linkedin_queue")

DEFAULT_QUEUE_FILE = "linkedin_queue.csv"

QUEUE_FIELDNAMES = [
    "lead_id",
    "company_name",
    "city",
    "state",
    "stream",
    "website",
    "priority_title",
    "linkedin_search_url",
    "status",
    "notes",
    "queued_date",
]


def make_linkedin_search_url(company_name: str, title: str) -> str:
    """Generate a LinkedIn people-search URL for a company + title.

    Opens the LinkedIn People search pre-filtered to the company name
    and a specific job title keyword.
    """
    query = f'"{company_name}" "{title}"'
    encoded = quote_plus(query)
    return (
        f"https://www.linkedin.com/search/results/people/"
        f"?keywords={encoded}&origin=GLOBAL_SEARCH_HEADER"
    )


def write_queue_file(
    leads_needing_lookup: list[dict],
    priority_titles_by_stream: dict[str, list[str]],
    queue_file: str = DEFAULT_QUEUE_FILE,
) -> int:
    """Append new entries to the LinkedIn queue CSV.

    Skips leads that already have an entry in the file (by lead_id),
    so re-running doesn't duplicate rows.

    Args:
        leads_needing_lookup:    Odoo lead dicts that need manual contact lookup.
        priority_titles_by_stream: {stream: [title, ...]} from YAML config.
        queue_file:              Path to the CSV queue file.

    Returns:
        Number of new rows written.
    """
    path = Path(queue_file)

    # Load existing lead IDs to avoid duplicates
    existing_ids: set[str] = set()
    if path.exists():
        try:
            with path.open("r", newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    existing_ids.add(str(row.get("lead_id", "")))
        except Exception as exc:
            logger.warning("Could not read existing queue file: %s", exc)

    is_new_file = not path.exists() or path.stat().st_size == 0
    written = 0

    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=QUEUE_FIELDNAMES)
        if is_new_file:
            writer.writeheader()

        for lead in leads_needing_lookup:
            lead_id = str(lead.get("id", ""))
            if lead_id in existing_ids:
                continue

            stream = lead.get("x_bd_stream") or ""
            titles = priority_titles_by_stream.get(stream, [])
            first_title = titles[0] if titles else "Owner"

            state_raw = lead.get("state_id")
            state = (
                state_raw[1]
                if isinstance(state_raw, (list, tuple)) and len(state_raw) == 2
                else ""
            )

            company = lead.get("partner_name") or lead.get("name") or ""
            writer.writerow({
                "lead_id": lead_id,
                "company_name": company,
                "city": lead.get("city") or "",
                "state": state,
                "stream": stream,
                "website": lead.get("website") or "",
                "priority_title": first_title,
                "linkedin_search_url": make_linkedin_search_url(company, first_title),
                "status": "pending",
                "notes": "",
                "queued_date": date.today().isoformat(),
            })
            existing_ids.add(lead_id)
            written += 1

    if written:
        logger.info("LinkedIn queue: wrote %d new row(s) to %s", written, path)
    return written


def format_queue_summary(queue_file: str = DEFAULT_QUEUE_FILE) -> str:
    """Return a text summary of the current queue file status."""
    path = Path(queue_file)
    if not path.exists():
        return "No LinkedIn queue file found."

    rows = []
    try:
        with path.open("r", newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
    except Exception as exc:
        return f"Could not read queue file: {exc}"

    total = len(rows)
    by_status: dict[str, int] = {}
    for row in rows:
        s = row.get("status", "pending")
        by_status[s] = by_status.get(s, 0) + 1

    pending = by_status.get("pending", 0)
    found = by_status.get("found", 0)
    skipped = by_status.get("skipped", 0)

    lines = [
        f"LinkedIn Queue: {path}",
        f"  Total: {total}  |  Pending: {pending}  |  Found: {found}  |  Skipped: {skipped}",
    ]
    if pending > 0:
        lines.append(f"  Action needed: open {path} and work through {pending} pending entries")
    return "\n".join(lines)
