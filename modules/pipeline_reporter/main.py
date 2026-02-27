"""Pipeline Reporter — Module 7.

Generates weekly and monthly pipeline reports in Markdown format.
Reports can be saved to a file and/or printed to stdout.

Usage:
    uv run python -m modules.pipeline_reporter.main
    uv run python -m modules.pipeline_reporter.main --type weekly
    uv run python -m modules.pipeline_reporter.main --type monthly
    uv run python -m modules.pipeline_reporter.main --stream stream_c
    uv run python -m modules.pipeline_reporter.main --output ~/reports/report.md
"""

import argparse
import os
import sys
from datetime import date
from typing import Optional

from dotenv import load_dotenv

from shared.config_loader import load_config
from shared.logger import get_logger
from shared.odoo_client import OdooClient

from modules.pipeline_reporter.reporter import (
    build_monthly_report,
    build_weekly_report,
)

load_dotenv()

logger = get_logger("pipeline_reporter")

TERMINAL_STAGES = {"Won", "Lost"}

LEAD_FIELDS = [
    "id", "name", "partner_name", "contact_name", "email_from",
    "city", "state_id", "stage_id",
    "x_bd_stream", "x_lead_score", "x_data_source",
    "x_enrichment_status", "x_last_personal_contact",
    "create_date", "write_date",
]


def fetch_all_leads(
    odoo: OdooClient,
    stream_filter: Optional[str] = None,
) -> list[dict]:
    """Fetch all leads from Odoo for reporting.

    Unlike other modules, the reporter reads ALL leads (including terminal)
    to compute full pipeline metrics.
    """
    domain = [
        ["active", "in", [True, False]],
    ]
    if stream_filter:
        domain.append(["x_bd_stream", "=", stream_filter])

    leads = odoo.search_leads(domain, fields=LEAD_FIELDS)
    logger.info("Fetched %d lead(s) for reporting", len(leads))
    return leads


def save_report(report: str, output_path: str) -> str:
    """Save the report to a file, creating directories as needed.

    Returns:
        The absolute path where the report was saved.
    """
    expanded = os.path.expanduser(output_path)
    abs_path = os.path.abspath(expanded)
    os.makedirs(os.path.dirname(abs_path), exist_ok=True)
    with open(abs_path, "w", encoding="utf-8") as f:
        f.write(report)
    logger.info("Report saved to %s", abs_path)
    return abs_path


def run(
    report_type: str = "weekly",
    stream_filter: Optional[str] = None,
    output_path: Optional[str] = None,
    reference_date: Optional[date] = None,
) -> dict:
    """Generate a pipeline report.

    Args:
        report_type: "weekly" or "monthly".
        stream_filter: Limit to one BD stream.
        output_path: File path to save the report. If None, uses config output_dir.
        reference_date: Date for the report (defaults to today).

    Returns:
        Summary dict: {report_type, lead_count, output_path, saved}
    """
    config = load_config("config/reporting.yaml")
    odoo = OdooClient.from_env()
    leads = fetch_all_leads(odoo, stream_filter)

    if reference_date is None:
        reference_date = date.today()

    if report_type == "monthly":
        report_config = config.get("monthly_report", {})
        report = build_monthly_report(leads, report_config, reference_date)
    else:
        report_config = config.get("weekly_report", {})
        report = build_weekly_report(leads, report_config, reference_date)

    # Print to stdout
    print()
    print(report)
    print()

    # Save to file
    saved = False
    final_path = None
    if output_path:
        final_path = save_report(report, output_path)
        saved = True
    else:
        output_dir = config.get("output_dir", "")
        if output_dir:
            date_str = reference_date.strftime("%Y-%m-%d")
            filename = f"bd-pipeline-{report_type}-{date_str}.md"
            final_path = save_report(report, os.path.join(output_dir, filename))
            saved = True

    # Summary
    print("=== Pipeline Reporter Summary ===")
    print(f"Report type: {report_type}  |  Leads: {len(leads)}")
    if saved and final_path:
        print(f"Saved to: {final_path}")
    else:
        print("Output: stdout only (no output_dir configured)")
    if stream_filter:
        print(f"Stream filter: {stream_filter}")
    print()

    logger.info(
        "Report complete | type=%s | leads=%d | saved=%s",
        report_type, len(leads), saved,
    )
    return {
        "report_type": report_type,
        "lead_count": len(leads),
        "output_path": final_path,
        "saved": saved,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Pipeline Reporter — generate BD pipeline summary reports."
    )
    parser.add_argument("--type", default="weekly", choices=["weekly", "monthly"],
                        dest="report_type", metavar="TYPE",
                        help="Report type: weekly or monthly (default: weekly).")
    parser.add_argument("--stream", default=None, metavar="STREAM",
                        help="Limit to one BD stream (e.g. stream_c).")
    parser.add_argument("--output", default=None, metavar="PATH",
                        help="File path to save the report (overrides config output_dir).")
    args = parser.parse_args()

    try:
        run(
            report_type=args.report_type,
            stream_filter=args.stream,
            output_path=args.output,
        )
    except EnvironmentError as exc:
        logger.error("%s", exc)
        sys.exit(1)
    except Exception as exc:
        logger.exception("Unexpected error: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
