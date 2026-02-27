"""Pipeline Reporter — pure reporting logic (no Odoo dependency).

Functions to compute pipeline metrics and format them as Markdown reports.
All functions take pre-fetched lead data and return computed results or
formatted strings.
"""

from collections import Counter
from datetime import date, timedelta

from shared.logger import get_logger

logger = get_logger("pipeline_reporter")

# Ordered pipeline stages for consistent display
PIPELINE_STAGES = [
    "Research", "Qualified", "Outreach", "Engaged",
    "Negotiating", "Proposal", "Samples Sent", "Won", "Lost", "Not Now",
]

# Stages considered terminal (excluded from "active" counts)
TERMINAL_STAGES = {"Won", "Lost"}

# Default thresholds
DEFAULT_STALE_DAYS = 21
DEFAULT_ATTENTION_SCORE = 60


def _resolve_many2one(value):
    """Extract the display name from an Odoo many2one field."""
    if isinstance(value, (list, tuple)) and len(value) == 2:
        return value[1]
    return value


def _stage_name(lead: dict) -> str:
    """Get the stage name string from a lead dict."""
    return _resolve_many2one(lead.get("stage_id")) or "Unknown"


def _parse_date(value) -> date | None:
    """Parse an ISO date string to a date object. Returns None on failure."""
    if not value or value is False:
        return None
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except (ValueError, TypeError):
        return None


# =========================================================================
# Metric computation functions
# =========================================================================

def pipeline_summary_by_stream(leads: list[dict]) -> dict[str, dict[str, int]]:
    """Count leads per stage per BD stream.

    Returns:
        {stream_name: {stage_name: count, ...}, ...}
    """
    result: dict[str, dict[str, int]] = {}
    for lead in leads:
        stream = lead.get("x_bd_stream") or "unassigned"
        stage = _stage_name(lead)
        if stream not in result:
            result[stream] = {}
        result[stream][stage] = result[stream].get(stage, 0) + 1
    return result


def new_leads_this_week(leads: list[dict], reference_date: date | None = None) -> list[dict]:
    """Filter leads created in the past 7 days.

    Args:
        leads: All leads with 'create_date' field.
        reference_date: Date to measure from (defaults to today).

    Returns:
        List of leads created within the last 7 days.
    """
    if reference_date is None:
        reference_date = date.today()
    cutoff = reference_date - timedelta(days=7)

    new = []
    for lead in leads:
        created = _parse_date(lead.get("create_date"))
        if created and created >= cutoff:
            new.append(lead)
    return new


def stale_leads(
    leads: list[dict],
    stale_days: int = DEFAULT_STALE_DAYS,
    reference_date: date | None = None,
) -> list[dict]:
    """Find leads with no activity in stale_days+ days.

    Uses write_date as proxy for last activity. Excludes terminal stages.

    Returns:
        List of stale leads, sorted by staleness (oldest first).
    """
    if reference_date is None:
        reference_date = date.today()
    cutoff = reference_date - timedelta(days=stale_days)

    stale = []
    for lead in leads:
        stage = _stage_name(lead)
        if stage in TERMINAL_STAGES:
            continue
        write = _parse_date(lead.get("write_date"))
        if write and write < cutoff:
            stale.append(lead)

    stale.sort(key=lambda l: _parse_date(l.get("write_date")) or date.min)
    return stale


def leads_needing_attention(
    leads: list[dict],
    min_score: int = DEFAULT_ATTENTION_SCORE,
) -> list[dict]:
    """Find leads with score >= min_score but still in Research stage.

    These are high-potential leads that haven't been advanced yet.
    """
    attention = []
    for lead in leads:
        stage = _stage_name(lead)
        score = lead.get("x_lead_score") or 0
        if stage == "Research" and score >= min_score:
            attention.append(lead)
    attention.sort(key=lambda l: l.get("x_lead_score", 0), reverse=True)
    return attention


def top_leads(leads: list[dict], n: int = 5) -> list[dict]:
    """Return the top N leads by score across all streams.

    Excludes terminal stages.
    """
    active = [l for l in leads if _stage_name(l) not in TERMINAL_STAGES]
    active.sort(key=lambda l: l.get("x_lead_score") or 0, reverse=True)
    return active[:n]


def score_distribution(leads: list[dict]) -> dict[str, int]:
    """Compute a histogram of lead scores in buckets.

    Returns:
        {"0-19": count, "20-39": count, ..., "80-100": count}
    """
    buckets = {"0-19": 0, "20-39": 0, "40-59": 0, "60-79": 0, "80-100": 0}
    for lead in leads:
        stage = _stage_name(lead)
        if stage in TERMINAL_STAGES:
            continue
        score = lead.get("x_lead_score") or 0
        if score < 20:
            buckets["0-19"] += 1
        elif score < 40:
            buckets["20-39"] += 1
        elif score < 60:
            buckets["40-59"] += 1
        elif score < 80:
            buckets["60-79"] += 1
        else:
            buckets["80-100"] += 1
    return buckets


def source_effectiveness(leads: list[dict]) -> dict[str, dict]:
    """Analyze which data sources produce the best leads.

    Groups leads by x_data_source and computes count and average score.

    Returns:
        {source: {"count": int, "avg_score": float}, ...}
    """
    by_source: dict[str, list[int]] = {}
    for lead in leads:
        source = lead.get("x_data_source") or "unknown"
        score = lead.get("x_lead_score") or 0
        if source not in by_source:
            by_source[source] = []
        by_source[source].append(score)

    result = {}
    for source, scores in sorted(by_source.items()):
        result[source] = {
            "count": len(scores),
            "avg_score": round(sum(scores) / len(scores), 1) if scores else 0,
        }
    return result


def conversion_funnel(leads: list[dict]) -> dict[str, int]:
    """Count leads at or past each funnel stage.

    A lead that reached "Outreach" also counts for "Research" and "Qualified".

    Returns:
        {stage_name: count_at_or_past, ...} for the main funnel stages.
    """
    funnel_stages = ["Research", "Qualified", "Outreach", "Engaged",
                     "Negotiating", "Proposal", "Samples Sent", "Won"]
    stage_index = {s: i for i, s in enumerate(funnel_stages)}

    counts = {s: 0 for s in funnel_stages}
    for lead in leads:
        stage = _stage_name(lead)
        if stage in ("Lost", "Not Now"):
            continue
        lead_idx = stage_index.get(stage)
        if lead_idx is None:
            continue
        # Count this lead at its stage and all prior stages
        for i in range(lead_idx + 1):
            counts[funnel_stages[i]] += 1

    return counts


# =========================================================================
# Markdown formatting
# =========================================================================

def format_pipeline_summary_table(summary: dict[str, dict[str, int]]) -> str:
    """Format pipeline summary as a Markdown table.

    Columns: Stage | Stream1 | Stream2 | ... | Total
    """
    if not summary:
        return "_No leads in pipeline._\n"

    streams = sorted(summary.keys())
    # Collect all stages present, ordered by PIPELINE_STAGES
    all_stages = set()
    for stage_counts in summary.values():
        all_stages.update(stage_counts.keys())
    ordered_stages = [s for s in PIPELINE_STAGES if s in all_stages]
    # Add any stages not in our ordered list
    for s in sorted(all_stages):
        if s not in ordered_stages:
            ordered_stages.append(s)

    # Header
    header = "| Stage | " + " | ".join(streams) + " | Total |"
    separator = "|---|" + "|".join(["---:" for _ in streams]) + "|---:|"

    rows = []
    stream_totals = {s: 0 for s in streams}
    grand_total = 0
    for stage in ordered_stages:
        parts = [f"| {stage}"]
        row_total = 0
        for stream in streams:
            count = summary[stream].get(stage, 0)
            parts.append(str(count))
            stream_totals[stream] += count
            row_total += count
        parts.append(str(row_total))
        grand_total += row_total
        rows.append(" | ".join(parts) + " |")

    # Totals row
    total_parts = ["| **Total**"]
    for stream in streams:
        total_parts.append(f"**{stream_totals[stream]}**")
    total_parts.append(f"**{grand_total}**")
    rows.append(" | ".join(total_parts) + " |")

    return "\n".join([header, separator] + rows) + "\n"


def format_lead_list(leads: list[dict], fields: list[str] | None = None) -> str:
    """Format a list of leads as Markdown bullet points."""
    if not leads:
        return "_None._\n"

    lines = []
    for lead in leads:
        company = lead.get("partner_name") or lead.get("name") or f"#{lead.get('id', '?')}"
        stage = _stage_name(lead)
        score = lead.get("x_lead_score") or 0
        stream = lead.get("x_bd_stream") or ""
        city = lead.get("city") or ""

        detail_parts = []
        if stream:
            detail_parts.append(stream)
        if stage:
            detail_parts.append(stage)
        if city:
            detail_parts.append(city)
        detail = " | ".join(detail_parts)

        line = f"- **{company}** (score: {score})"
        if detail:
            line += f" — {detail}"
        lines.append(line)

    return "\n".join(lines) + "\n"


def format_score_distribution(dist: dict[str, int]) -> str:
    """Format score distribution as a simple text histogram."""
    if not any(dist.values()):
        return "_No scored leads._\n"

    max_count = max(dist.values()) if dist else 1
    lines = []
    for bucket, count in dist.items():
        bar_len = int(count / max_count * 20) if max_count > 0 else 0
        bar = "█" * bar_len
        lines.append(f"  {bucket:>6s}: {bar} {count}")
    return "\n".join(lines) + "\n"


def format_source_effectiveness(sources: dict[str, dict]) -> str:
    """Format source effectiveness as a Markdown table."""
    if not sources:
        return "_No source data._\n"

    header = "| Source | Leads | Avg Score |"
    separator = "|---|---:|---:|"
    rows = []
    for source, stats in sorted(sources.items(), key=lambda x: x[1]["avg_score"], reverse=True):
        rows.append(f"| {source} | {stats['count']} | {stats['avg_score']} |")

    return "\n".join([header, separator] + rows) + "\n"


def format_conversion_funnel(funnel: dict[str, int]) -> str:
    """Format conversion funnel as a visual funnel."""
    if not funnel or not any(funnel.values()):
        return "_No funnel data._\n"

    first_val = next((v for v in funnel.values() if v > 0), 1)
    lines = []
    for stage, count in funnel.items():
        pct = round(count / first_val * 100) if first_val > 0 else 0
        bar_len = int(pct / 5) if pct > 0 else 0
        bar = "█" * bar_len
        lines.append(f"  {stage:<14s}: {bar} {count} ({pct}%)")
    return "\n".join(lines) + "\n"


# =========================================================================
# Full report assembly
# =========================================================================

def build_weekly_report(
    leads: list[dict],
    config: dict,
    reference_date: date | None = None,
) -> str:
    """Build a full weekly pipeline report in Markdown.

    Args:
        leads: All leads fetched from Odoo.
        config: The weekly_report section from reporting.yaml.
        reference_date: Date for the report header (defaults to today).

    Returns:
        Complete Markdown report string.
    """
    if reference_date is None:
        reference_date = date.today()

    include = config.get("include", [])
    sections = []

    sections.append(f"# BD Pipeline Report — Week of {reference_date.strftime('%B %-d, %Y')}")
    sections.append("")

    # Quick stats
    active = [l for l in leads if _stage_name(l) not in TERMINAL_STAGES]
    won = [l for l in leads if _stage_name(l) == "Won"]
    lost = [l for l in leads if _stage_name(l) == "Lost"]
    sections.append(
        f"**Active leads:** {len(active)}  |  "
        f"**Won:** {len(won)}  |  **Lost:** {len(lost)}  |  "
        f"**Total:** {len(leads)}"
    )
    sections.append("")

    if "pipeline_summary_by_stream" in include:
        sections.append("## Pipeline by Stream")
        sections.append("")
        summary = pipeline_summary_by_stream(leads)
        sections.append(format_pipeline_summary_table(summary))

    if "new_leads_this_week" in include:
        sections.append("## New Leads This Week")
        sections.append("")
        new = new_leads_this_week(leads, reference_date)
        if new:
            sections.append(f"**{len(new)} new lead(s)** added in the past 7 days:")
            sections.append("")
        sections.append(format_lead_list(new))

    if "leads_needing_attention" in include:
        sections.append("## Leads Needing Attention")
        sections.append(f"_Scored {DEFAULT_ATTENTION_SCORE}+ but still in Research stage._")
        sections.append("")
        attention = leads_needing_attention(leads)
        sections.append(format_lead_list(attention))

    if "stale_leads" in include:
        sections.append("## Stale Leads")
        sections.append(f"_No activity in {DEFAULT_STALE_DAYS}+ days._")
        sections.append("")
        stale = stale_leads(leads, reference_date=reference_date)
        sections.append(format_lead_list(stale))

    if "top_5_leads" in include:
        sections.append("## Top 5 Leads by Score")
        sections.append("")
        top = top_leads(leads, n=5)
        sections.append(format_lead_list(top))

    sections.append("---")
    sections.append("_Generated by BD Automation Suite_")

    return "\n".join(sections)


def build_monthly_report(
    leads: list[dict],
    config: dict,
    reference_date: date | None = None,
) -> str:
    """Build a full monthly pipeline report in Markdown.

    Includes all weekly metrics plus conversion funnel, score distribution,
    and source effectiveness.

    Args:
        leads: All leads fetched from Odoo.
        config: The monthly_report section from reporting.yaml.
        reference_date: Date for the report header (defaults to today).

    Returns:
        Complete Markdown report string.
    """
    if reference_date is None:
        reference_date = date.today()

    include = config.get("include", [])
    expand_weekly = "all_weekly_metrics" in include

    sections = []

    sections.append(f"# BD Pipeline Report — {reference_date.strftime('%B %Y')}")
    sections.append("")

    # Quick stats
    active = [l for l in leads if _stage_name(l) not in TERMINAL_STAGES]
    won = [l for l in leads if _stage_name(l) == "Won"]
    lost = [l for l in leads if _stage_name(l) == "Lost"]
    sections.append(
        f"**Active leads:** {len(active)}  |  "
        f"**Won:** {len(won)}  |  **Lost:** {len(lost)}  |  "
        f"**Total:** {len(leads)}"
    )
    sections.append("")

    # Weekly metrics (if all_weekly_metrics is specified or individual ones)
    if expand_weekly or "pipeline_summary_by_stream" in include:
        sections.append("## Pipeline by Stream")
        sections.append("")
        summary = pipeline_summary_by_stream(leads)
        sections.append(format_pipeline_summary_table(summary))

    if expand_weekly or "top_5_leads" in include:
        sections.append("## Top 5 Leads by Score")
        sections.append("")
        top = top_leads(leads, n=5)
        sections.append(format_lead_list(top))

    if expand_weekly or "leads_needing_attention" in include:
        sections.append("## Leads Needing Attention")
        sections.append(f"_Scored {DEFAULT_ATTENTION_SCORE}+ but still in Research stage._")
        sections.append("")
        attention = leads_needing_attention(leads)
        sections.append(format_lead_list(attention))

    if expand_weekly or "stale_leads" in include:
        sections.append("## Stale Leads")
        sections.append(f"_No activity in {DEFAULT_STALE_DAYS}+ days._")
        sections.append("")
        stale = stale_leads(leads, reference_date=reference_date)
        sections.append(format_lead_list(stale))

    # Monthly-only metrics
    if "conversion_rates" in include:
        sections.append("## Conversion Funnel")
        sections.append("")
        funnel = conversion_funnel(leads)
        sections.append(format_conversion_funnel(funnel))

    if "score_distribution" in include:
        sections.append("## Score Distribution")
        sections.append("")
        dist = score_distribution(leads)
        sections.append(format_score_distribution(dist))

    if "source_effectiveness" in include:
        sections.append("## Source Effectiveness")
        sections.append("")
        sources = source_effectiveness(leads)
        sections.append(format_source_effectiveness(sources))

    sections.append("---")
    sections.append("_Generated by BD Automation Suite_")

    return "\n".join(sections)
