#!/usr/bin/env python3
"""Onboard a new client by distributing their config into the suite's YAML files.

Usage:
    uv run python scripts/onboard_client.py acme_foods          # from config/clients/acme_foods.yaml
    uv run python scripts/onboard_client.py acme_foods --dry-run # preview changes without writing
"""

import argparse
import sys
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_DIR = PROJECT_ROOT / "config"
CLIENTS_DIR = CONFIG_DIR / "clients"


def load_yaml(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def save_yaml(path: Path, data: dict) -> None:
    """Write a new YAML file from scratch (only used for new stream config files)."""
    with path.open("w", encoding="utf-8") as f:
        yaml.dump(data, f, default_flow_style=False, sort_keys=False, allow_unicode=True)


def _dump_fragment(top_key: str, stream: str, data) -> str:
    """Serialize a single stream section as a YAML text fragment.

    Returns something like:
        # --- Client: acme_foods ---
        scoring_rules:
          acme_foods:
            criteria:
              ...
    But only the inner part (indented under the top key) so we can
    append just the stream block to the existing file.
    """
    # Dump the stream data with proper indentation
    inner = yaml.dump(
        {stream: data},
        default_flow_style=False,
        sort_keys=False,
        allow_unicode=True,
    )
    # Indent each line by 2 spaces so it nests under the top-level key
    indented = "\n".join(f"  {line}" if line.strip() else line for line in inner.splitlines())
    return indented


def _append_section(
    config_file: Path,
    top_key: str,
    stream: str,
    data,
    changes: list[str],
    dry_run: bool,
) -> None:
    """Append a stream's YAML block to an existing config file, preserving all comments."""
    # Check if stream already exists (parse to verify, but don't rewrite)
    config = load_yaml(config_file)
    section = config.get(top_key, {})

    if section and stream in section:
        changes.append(f"  SKIP  {config_file.name} — stream '{stream}' already exists under {top_key}")
        return

    # Build the text fragment to append
    fragment = _dump_fragment(top_key, stream, data)
    comment = f"\n  # --- Client stream: {stream} ---\n"

    changes.append(f"  APPEND {config_file.name} — added '{stream}' to {top_key}")
    if not dry_run:
        with config_file.open("a", encoding="utf-8") as f:
            f.write(comment)
            f.write(fragment)
            f.write("\n")


def _append_outreach(
    config_file: Path,
    stream: str,
    outreach_data,
    llm_overrides: dict | None,
    changes: list[str],
    dry_run: bool,
) -> None:
    """Append outreach template and optional LLM overrides for a stream."""
    config = load_yaml(config_file)
    templates = config.get("templates", {})

    if templates and stream in templates:
        changes.append(f"  SKIP  outreach.yaml — stream '{stream}' already exists")
        return

    # Build template fragment under "templates:"
    template_fragment = _dump_fragment("templates", stream, outreach_data)
    comment = f"\n  # --- Client stream: {stream} ---\n"

    parts = [comment, template_fragment, "\n"]

    # Add LLM overrides if provided
    if llm_overrides:
        llm_fragment = _dump_fragment("llm_overrides", stream, llm_overrides)
        parts.append(f"\n# LLM overrides for {stream}\nllm_overrides:\n")
        # Only append the llm_overrides top key if it doesn't exist yet
        existing_text = config_file.read_text(encoding="utf-8")
        if "llm_overrides:" in existing_text:
            # Top key exists — just append the stream block indented
            parts[-1] = f"\n  # LLM overrides: {stream}\n"
            parts.append(llm_fragment)
        else:
            parts.append(llm_fragment)
        parts.append("\n")

    changes.append(f"  APPEND outreach.yaml — added stream '{stream}'")
    if not dry_run:
        with config_file.open("a", encoding="utf-8") as f:
            f.writelines(parts)


def _append_reporting(
    config_file: Path,
    stream: str,
    reporting_data: dict,
    changes: list[str],
    dry_run: bool,
) -> None:
    """Append client reporting config."""
    config = load_yaml(config_file)
    client_reports = config.get("client_reports", {})

    if client_reports and stream in client_reports:
        changes.append(f"  SKIP  reporting.yaml — stream '{stream}' already exists")
        return

    # If client_reports key doesn't exist yet, add it
    existing_text = config_file.read_text(encoding="utf-8")
    if "client_reports:" not in existing_text:
        header = "\n# Per-client report delivery\nclient_reports:\n"
    else:
        header = ""

    fragment = _dump_fragment("client_reports", stream, reporting_data)
    comment = f"\n  # --- Client stream: {stream} ---\n"

    changes.append(f"  APPEND reporting.yaml — added stream '{stream}'")
    if not dry_run:
        with config_file.open("a", encoding="utf-8") as f:
            if header:
                f.write(header)
            f.write(comment)
            f.write(fragment)
            f.write("\n")


def onboard(client_name: str, dry_run: bool = False) -> None:
    client_file = CLIENTS_DIR / f"{client_name}.yaml"
    if not client_file.exists():
        print(f"Error: {client_file} not found.")
        print(f"Copy config/clients/_template.yaml to config/clients/{client_name}.yaml and fill it in.")
        sys.exit(1)

    client = load_yaml(client_file)
    stream = client.get("stream_name")
    if not stream:
        print("Error: stream_name is required in client config.")
        sys.exit(1)

    display_name = client.get("client_name", stream)
    print(f"Onboarding client: {display_name} (stream: {stream})")
    if dry_run:
        print("--- DRY RUN — no files will be modified ---\n")

    changes: list[str] = []

    # 1. Create stream config (prospect research) — new file, no comments to preserve
    stream_file = CONFIG_DIR / f"{stream}.yaml"
    if stream_file.exists():
        print(f"  SKIP  {stream_file.name} already exists")
    else:
        prospect = client.get("prospect_research", {})
        stream_config = {"stream": stream, **prospect}
        changes.append(f"  CREATE {stream_file.name}")
        if not dry_run:
            save_yaml(stream_file, stream_config)

    # 2. Inject into scoring.yaml
    _append_section(
        config_file=CONFIG_DIR / "scoring.yaml",
        top_key="scoring_rules",
        stream=stream,
        data=client.get("scoring", {}),
        changes=changes,
        dry_run=dry_run,
    )

    # 3. Inject into enrichment.yaml
    _append_section(
        config_file=CONFIG_DIR / "enrichment.yaml",
        top_key="enrichment_sources",
        stream=stream,
        data=client.get("enrichment", []),
        changes=changes,
        dry_run=dry_run,
    )

    # 4. Inject into contact_discovery.yaml
    _append_section(
        config_file=CONFIG_DIR / "contact_discovery.yaml",
        top_key="target_titles",
        stream=stream,
        data=client.get("contact_discovery", {}),
        changes=changes,
        dry_run=dry_run,
    )

    # 5. Inject into outreach.yaml
    _append_outreach(
        config_file=CONFIG_DIR / "outreach.yaml",
        stream=stream,
        outreach_data=client.get("outreach", {}),
        llm_overrides=client.get("llm") or None,
        changes=changes,
        dry_run=dry_run,
    )

    # 6. Inject into reporting.yaml
    _append_reporting(
        config_file=CONFIG_DIR / "reporting.yaml",
        stream=stream,
        reporting_data=client.get("reporting", {}),
        changes=changes,
        dry_run=dry_run,
    )

    # Print summary
    print()
    for c in changes:
        print(c)

    # Print Odoo checklist
    print(f"""
--- Odoo Setup Checklist for '{display_name}' ---

  1. Verify x_bd_stream value '{stream}' is recognized in your Odoo pipeline
     (or create a dedicated pipeline for this client if volume warrants it)

  2. Confirm these custom fields exist on crm.lead (run setup_odoo_fields.py
     if this is a fresh Odoo instance):
       x_bd_stream, x_lead_score, x_enrichment_status, x_outreach_draft,
       x_last_personal_contact, x_manual_intervention

  3. Add any client-specific custom fields referenced in their scoring criteria
     that don't already exist

  4. Set GEMINI_API_KEY in .env if using Gemini models for this client's LLM tasks

  5. Test with dry run:
       uv run python -m modules.prospect_research.main --stream {stream} --dry-run --limit 5
       uv run python -m modules.lead_scoring.main --dry-run
       uv run python -m modules.outreach_drafter.main --dry-run

  6. When satisfied, run the full pipeline:
       uv run python -m modules.prospect_research.main --stream {stream} --limit 20
""")

    if dry_run:
        print("--- DRY RUN complete. No files were modified. ---")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Onboard a new client to the BD Automation Suite")
    parser.add_argument("client", help="Client config name (matches config/clients/{name}.yaml)")
    parser.add_argument("--dry-run", action="store_true", help="Preview changes without writing files")
    args = parser.parse_args()
    onboard(args.client, dry_run=args.dry_run)
