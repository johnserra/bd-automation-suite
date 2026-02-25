#!/usr/bin/env python3
"""
One-time setup script: create custom Odoo fields and pipeline stages.

Usage:
    uv run python scripts/setup_odoo_fields.py
    uv run python scripts/setup_odoo_fields.py --dry-run

The script is idempotent — safe to run multiple times. It checks whether each
field / stage already exists before creating it and prints a clear summary.
"""

import argparse
import os
import sys
import xmlrpc.client
from datetime import datetime

# ---------------------------------------------------------------------------
# Field definitions — matches spec §3.2
# ---------------------------------------------------------------------------

CUSTOM_FIELDS = [
    {
        "name": "x_bd_stream",
        "field_description": "BD Stream",
        "ttype": "selection",
        "selection": "[('stream_a','Stream A'),('stream_b','Stream B'),('stream_c','Stream C')]",
    },
    {
        "name": "x_business_type",
        "field_description": "Business Type",
        "ttype": "selection",
        "selection": "[('type_1','Type 1'),('type_2','Type 2'),('type_3','Type 3'),('type_4','Type 4'),('type_5','Type 5'),('other','Other')]",
    },
    {
        "name": "x_product_interest",
        "field_description": "Product Interest",
        "ttype": "selection",
        "selection": "[('program_1','Program 1'),('program_2','Program 2'),('program_3','Program 3'),('multiple','Multiple')]",
    },
    {
        "name": "x_lead_score",
        "field_description": "Lead Score",
        "ttype": "integer",
    },
    {
        "name": "x_score_breakdown",
        "field_description": "Score Breakdown (JSON)",
        "ttype": "text",
    },
    {
        "name": "x_already_importing",
        "field_description": "Already Importing from Overseas",
        "ttype": "boolean",
    },
    {
        "name": "x_current_supplier",
        "field_description": "Current Supplier(s)",
        "ttype": "char",
    },
    {
        "name": "x_import_source_country",
        "field_description": "Import Source Country",
        "ttype": "char",
    },
    {
        "name": "x_decision_maker_title",
        "field_description": "Decision Maker Title",
        "ttype": "char",
    },
    {
        "name": "x_linkedin_url",
        "field_description": "LinkedIn Profile URL",
        "ttype": "char",
    },
    {
        "name": "x_company_linkedin",
        "field_description": "Company LinkedIn URL",
        "ttype": "char",
    },
    {
        "name": "x_company_size",
        "field_description": "Company Size",
        "ttype": "selection",
        "selection": "[('small','Small (<$5M)'),('medium','Medium ($5M-$50M)'),('large','Large ($50M-$500M)'),('enterprise','Enterprise (>$500M)')]",
    },
    {
        "name": "x_data_source",
        "field_description": "Data Source",
        "ttype": "char",
    },
    {
        "name": "x_last_personal_contact",
        "field_description": "Last Personal Contact",
        "ttype": "date",
    },
    {
        "name": "x_sample_sent_date",
        "field_description": "Sample Sent Date",
        "ttype": "date",
    },
    {
        "name": "x_enrichment_date",
        "field_description": "Enrichment Date",
        "ttype": "date",
    },
    {
        "name": "x_enrichment_status",
        "field_description": "Enrichment Status",
        "ttype": "selection",
        "selection": "[('pending','Pending'),('partial','Partial'),('complete','Complete')]",
    },
    {
        "name": "x_outreach_draft",
        "field_description": "Outreach Draft",
        "ttype": "text",
    },
    {
        "name": "x_property_type",
        "field_description": "Property Type",
        "ttype": "selection",
        "selection": "[('surface_lot','Surface Lot'),('garage','Parking Garage'),('mixed_use','Mixed-Use'),('event_venue','Event Venue')]",
    },
    {
        "name": "x_estimated_spaces",
        "field_description": "Estimated Spaces",
        "ttype": "integer",
    },
    {
        "name": "x_current_operator",
        "field_description": "Current Parking Operator",
        "ttype": "char",
    },
]

# Pipeline stages — spec §3.3, in display order
PIPELINE_STAGES = [
    {"name": "Research",    "description": "Lead identified, needs enrichment"},
    {"name": "Qualified",   "description": "Enriched and scored, ready for outreach"},
    {"name": "Outreach",    "description": "Initial contact made"},
    {"name": "Engaged",     "description": "Two-way conversation happening"},
    {"name": "Samples Sent","description": "Product samples shipped (Packaging)"},
    {"name": "Site Visit",  "description": "Physical visit scheduled or completed (Parking)"},
    {"name": "Proposal",    "description": "Formal proposal sent"},
    {"name": "Negotiating", "description": "Active negotiation"},
    {"name": "Won",         "description": "Deal closed"},
    {"name": "Not Now",     "description": "Timing isn't right, revisit later"},
    {"name": "Lost",        "description": "Dead lead"},
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def connect():
    """Authenticate with Odoo and return (uid, models_proxy)."""
    required = ["ODOO_URL", "ODOO_DB", "ODOO_USER", "ODOO_API_KEY"]
    missing = [v for v in required if not os.getenv(v)]
    if missing:
        print(f"ERROR: Missing env vars: {', '.join(missing)}")
        print("Copy .env.example → .env and fill in your Odoo credentials.")
        sys.exit(1)

    url = os.environ["ODOO_URL"].rstrip("/")
    db = os.environ["ODOO_DB"]
    user = os.environ["ODOO_USER"]
    api_key = os.environ["ODOO_API_KEY"]

    common = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/common")
    uid = common.authenticate(db, user, api_key, {})
    if not uid:
        print("ERROR: Odoo authentication failed. Check ODOO_USER and ODOO_API_KEY.")
        sys.exit(1)

    models = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/object")
    return uid, models, db, api_key


def execute(models, db, uid, api_key, model, method, *args, **kwargs):
    return models.execute_kw(db, uid, api_key, model, method, list(args), kwargs)


# ---------------------------------------------------------------------------
# Main logic
# ---------------------------------------------------------------------------

def setup_fields(models, db, uid, api_key, dry_run: bool):
    print("\n=== Custom Fields (crm.lead) ===")

    # Get the ir.model ID for crm.lead
    model_ids = execute(models, db, uid, api_key, "ir.model", "search",
                        [["model", "=", "crm.lead"]])
    if not model_ids:
        print("ERROR: crm.lead model not found in Odoo. Is CRM installed?")
        sys.exit(1)
    crm_lead_model_id = model_ids[0]

    created = skipped = errors = 0

    for field_def in CUSTOM_FIELDS:
        name = field_def["name"]
        existing = execute(models, db, uid, api_key, "ir.model.fields", "search",
                           [["model_id", "=", crm_lead_model_id], ["name", "=", name]])

        if existing:
            print(f"  ↷ {name} — already exists")
            skipped += 1
            continue

        if dry_run:
            print(f"  [DRY RUN] Would create: {name} ({field_def['ttype']})")
            created += 1
            continue

        try:
            values = {
                "model_id": crm_lead_model_id,
                "name": name,
                "field_description": field_def["field_description"],
                "ttype": field_def["ttype"],
            }
            if "selection" in field_def:
                values["selection"] = field_def["selection"]

            execute(models, db, uid, api_key, "ir.model.fields", "create", values)
            print(f"  ✓ {name} — created")
            created += 1
        except Exception as exc:
            print(f"  ✗ {name} — ERROR: {exc}")
            errors += 1

    print(f"\n  Fields: {created} {'would be ' if dry_run else ''}created, "
          f"{skipped} already exist, {errors} errors")
    return errors


def setup_stages(models, db, uid, api_key, dry_run: bool):
    print("\n=== Pipeline Stages (crm.stage) ===")

    created = skipped = errors = 0

    for i, stage in enumerate(PIPELINE_STAGES):
        name = stage["name"]
        existing = execute(models, db, uid, api_key, "crm.stage", "search",
                           [["name", "=", name]])

        if existing:
            print(f"  ↷ {name} — already exists")
            skipped += 1
            continue

        if dry_run:
            print(f"  [DRY RUN] Would create stage: {name}")
            created += 1
            continue

        try:
            execute(models, db, uid, api_key, "crm.stage", "create", {
                "name": name,
                "sequence": (i + 1) * 10,
                "description": stage.get("description", ""),
                "is_won": name == "Won",
            })
            print(f"  ✓ {name} — created")
            created += 1
        except Exception as exc:
            print(f"  ✗ {name} — ERROR: {exc}")
            errors += 1

    print(f"\n  Stages: {created} {'would be ' if dry_run else ''}created, "
          f"{skipped} already exist, {errors} errors")
    return errors


def main():
    parser = argparse.ArgumentParser(
        description="Create custom Odoo fields and pipeline stages for BD Automation Suite."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview changes without writing to Odoo.",
    )
    args = parser.parse_args()

    # Load .env if present
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass  # dotenv optional at this stage

    if args.dry_run:
        print("=== DRY RUN — no changes will be made ===")

    print(f"Connecting to Odoo at {os.getenv('ODOO_URL', '(not set)')} ...")
    uid, models, db, api_key = connect()
    print(f"Authenticated as uid={uid}")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    field_errors = setup_fields(models, db, uid, api_key, args.dry_run)
    stage_errors = setup_stages(models, db, uid, api_key, args.dry_run)

    total_errors = field_errors + stage_errors
    print(f"\n{'=== DRY RUN COMPLETE ===' if args.dry_run else '=== SETUP COMPLETE ==='}")
    if total_errors:
        print(f"WARNING: {total_errors} error(s) occurred. Review output above.")
        sys.exit(1)
    else:
        print("All fields and stages are ready.")


if __name__ == "__main__":
    main()
