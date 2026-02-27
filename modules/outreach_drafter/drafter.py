"""Outreach Drafter — pure drafting logic (no Odoo dependency).

Functions to assemble lead context, select templates, build prompts,
and generate outreach drafts via LLM.
"""

from shared.logger import get_logger

logger = get_logger("outreach_drafter")

# Fields to include in the lead context block for the LLM prompt.
CONTEXT_FIELDS = [
    ("partner_name", "Company"),
    ("contact_name", "Contact Name"),
    ("email_from", "Email"),
    ("city", "City"),
    ("state_id", "State"),
    ("description", "Background"),
    ("x_bd_stream", "BD Stream"),
    ("x_business_type", "Business Type"),
    ("x_current_supplier", "Current Supplier"),
    ("x_import_source_country", "Import Source Country"),
    ("x_estimated_spaces", "Estimated Spaces"),
    ("x_current_operator", "Current Operator"),
    ("x_property_type", "Property Type"),
    ("x_company_size", "Company Size"),
    ("x_lead_score", "Lead Score"),
    ("x_decision_maker_title", "Decision Maker Title"),
]

# Map Odoo stage names to template keys.
STAGE_TEMPLATE_MAP = {
    "Qualified": "initial_contact",
    "Samples Sent": "sample_followup",
}


def _resolve_many2one(value):
    """Extract the display name from an Odoo many2one field.

    Odoo returns many2one fields as [id, "Name"] tuples.
    Returns the name string, or the value unchanged if not a many2one.
    """
    if isinstance(value, (list, tuple)) and len(value) == 2:
        return value[1]
    return value


def assemble_lead_context(lead: dict) -> str:
    """Format lead fields as a structured key-value block for the LLM prompt.

    Handles Odoo many2one fields (stage_id, state_id → extract name string).
    Omits fields that are empty/False.
    """
    lines = []
    for field_name, label in CONTEXT_FIELDS:
        raw = lead.get(field_name)
        if raw is False or raw is None or raw == "":
            continue
        value = _resolve_many2one(raw)
        if value is False or value is None or value == "":
            continue
        lines.append(f"{label}: {value}")
    return "\n".join(lines)


def select_template(
    stream: str, stage_name: str, templates: dict
) -> tuple[str, dict] | None:
    """Select the appropriate outreach template for a lead.

    Args:
        stream: BD stream name (e.g. "stream_c").
        stage_name: Current pipeline stage name (e.g. "Qualified").
        templates: The templates dict from outreach.yaml.

    Returns:
        (template_key, template_dict) or None if no matching template.
    """
    template_key = STAGE_TEMPLATE_MAP.get(stage_name)
    if not template_key:
        return None

    stream_templates = templates.get(stream)
    if not stream_templates:
        return None

    template = stream_templates.get(template_key)
    if not template:
        return None

    return (template_key, template)


def build_prompt(lead_context: str, template: dict) -> str:
    """Construct the user-turn prompt from template fields and lead context.

    Includes: tone, max_length, structure (as numbered list),
    context_to_include hints, and example_subject_lines.
    """
    parts = []

    parts.append("Write a personalized outreach email based on the lead information below.")
    parts.append("")

    # Tone
    tone = template.get("tone", "")
    if tone:
        parts.append(f"Tone: {tone}")

    # Max length
    max_length = template.get("max_length")
    if max_length:
        parts.append(f"Maximum length: {max_length} words")

    # Structure
    structure = template.get("structure", [])
    if structure:
        parts.append("")
        parts.append("Email structure:")
        for i, item in enumerate(structure, 1):
            parts.append(f"  {i}. {item}")

    # Context hints
    context_to_include = template.get("context_to_include", [])
    if context_to_include:
        parts.append("")
        parts.append("Context to incorporate:")
        for hint in context_to_include:
            parts.append(f"  - {hint}")

    # Example subject lines
    example_subjects = template.get("example_subject_lines", [])
    if example_subjects:
        parts.append("")
        parts.append("Example subject line patterns:")
        for ex in example_subjects:
            parts.append(f"  - {ex}")

    # Lead context block
    parts.append("")
    parts.append("--- Lead Information ---")
    parts.append(lead_context)
    parts.append("--- End Lead Information ---")

    return "\n".join(parts)


def draft_outreach(
    lead: dict,
    template_key: str,
    template: dict,
    llm,
    system_prompt: str,
    llm_model: str,
    max_tokens: int,
) -> str:
    """Generate an outreach email draft for a lead.

    Calls assemble_lead_context → build_prompt → llm.complete().
    Appends a metadata footer identifying the template used.

    Args:
        lead: Odoo lead dict.
        template_key: Template key (e.g. "initial_contact").
        template: Template dict from outreach.yaml.
        llm: LLMClient instance.
        system_prompt: System prompt for the LLM.
        llm_model: Model ID to use.
        max_tokens: Max output tokens for LLM.

    Returns:
        Full draft string with metadata footer.
    """
    stream = lead.get("x_bd_stream", "unknown")
    lead_context = assemble_lead_context(lead)
    prompt = build_prompt(lead_context, template)

    draft = llm.complete(
        prompt=prompt,
        system=system_prompt,
        model=llm_model,
        max_tokens=max_tokens,
    )

    footer = f"\n---\n[Drafted by AI — template: {stream}/{template_key}]"
    return draft + footer
