"""Pure lead-scoring logic for Module 4.

No Odoo dependency — all functions take plain dicts and return plain values,
making them trivial to unit-test.

Condition string syntax (as written in scoring.yaml):

    Condition string          Meaning
    ─────────────────────     ──────────────────────────────────────────────
    is not empty              field is truthy (non-empty string, non-zero, True)
    is empty                  field is falsy (False, None, "", 0)
    == true                   field is boolean True (or 1)
    == false                  field is boolean False (or 0 or falsy)
    == 'some_string'          field equals 'some_string'
    in ['a', 'b', 'c']        field value is a member of the list
    >= N  /  <= N  /  > N  /  < N   numeric comparison (field must be numeric)

Negative points are supported: set points to a negative integer in the YAML.

Special field handling:
    state_id   Odoo returns [42, "New York"]. resolve_field_value() maps the
               integer ID to the two-letter state code using a pre-built cache
               so that condition "in ['NY', 'PA']" works as expected.
    Other many2ones (stage_id, etc.) → the name string is extracted.
"""

import ast
import json
import re
from typing import Any, Optional


# ---------------------------------------------------------------------------
# Field-value resolution
# ---------------------------------------------------------------------------

def resolve_field_value(
    field_name: str,
    raw_value: Any,
    state_code_cache: dict[int, str],
) -> Any:
    """Normalize an Odoo field value for condition evaluation.

    Handles:
    - Odoo's False (unset) → None
    - Many2one [id, "Name"] → name string, with special-casing for state_id
    - Everything else → as-is

    Args:
        field_name:       The Odoo field name, e.g. 'state_id'.
        raw_value:        The raw value from Odoo search_read.
        state_code_cache: {state_id_int → "NY"} built at scorer startup.
    """
    # Odoo uses False for unset fields of most types
    if raw_value is False or raw_value is None:
        return None

    # Many2one: [id, "Name"]
    if isinstance(raw_value, (list, tuple)) and len(raw_value) == 2:
        odoo_id, name = raw_value
        if field_name == "state_id" and isinstance(odoo_id, int):
            # Return state code ("NY") rather than full name ("New York")
            return state_code_cache.get(odoo_id, name)
        return name

    return raw_value


# ---------------------------------------------------------------------------
# Condition evaluation
# ---------------------------------------------------------------------------

def evaluate_condition(condition_str: str, field_value: Any) -> bool:
    """Evaluate a YAML condition string against a resolved field value.

    Returns True if the condition is satisfied, False otherwise.
    An unparseable condition string returns False and is logged at warning
    level by the caller.

    Robustness: if the field_value is None/missing and the condition requires
    a value (e.g. '>= 50'), returns False rather than raising.
    """
    c = condition_str.strip()

    # ── Emptiness checks ────────────────────────────────────────────────────
    if c == "is not empty":
        return _is_not_empty(field_value)

    if c == "is empty":
        return not _is_not_empty(field_value)

    # ── Boolean literals ─────────────────────────────────────────────────────
    if c == "== true":
        return field_value is True or field_value == 1

    if c == "== false":
        return not _is_not_empty(field_value)

    # ── String equality: == 'value' ──────────────────────────────────────────
    m = re.fullmatch(r"==\s*'([^']*)'", c)
    if m:
        if field_value is None:
            return False
        return str(field_value) == m.group(1)

    # ── Membership: in ['a', 'b', ...] ──────────────────────────────────────
    m = re.fullmatch(r"in\s+(\[.+\])", c, re.DOTALL)
    if m:
        if field_value is None:
            return False
        try:
            allowed = ast.literal_eval(m.group(1))
        except (ValueError, SyntaxError):
            return False
        return field_value in allowed

    # ── Numeric comparisons: >= N, <= N, > N, < N ───────────────────────────
    m = re.fullmatch(r"(>=|<=|>|<)\s*(-?\d+(?:\.\d+)?)", c)
    if m:
        op, num_str = m.group(1), m.group(2)
        if field_value is None:
            return False
        try:
            fv = float(field_value)
            num = float(num_str)
        except (TypeError, ValueError):
            return False
        return _numeric_compare(fv, op, num)

    # ── Unknown condition ────────────────────────────────────────────────────
    return False


def _is_not_empty(value: Any) -> bool:
    """Return True when the value is meaningfully present."""
    if value is None or value is False:
        return False
    if isinstance(value, str):
        return value.strip() != ""
    if isinstance(value, (int, float)):
        return True        # 0 is a valid integer value; only None/False = empty
    return bool(value)


def _numeric_compare(value: float, op: str, threshold: float) -> bool:
    if op == ">=":
        return value >= threshold
    if op == "<=":
        return value <= threshold
    if op == ">":
        return value > threshold
    if op == "<":
        return value < threshold
    return False


# ---------------------------------------------------------------------------
# Lead scoring
# ---------------------------------------------------------------------------

def score_lead(
    lead: dict,
    criteria: list[dict],
    state_code_cache: dict[int, str],
) -> tuple[int, dict]:
    """Compute a score for a single lead against a list of criteria.

    Args:
        lead:             Odoo lead dict from search_read.
        criteria:         List of criterion dicts from scoring.yaml.
        state_code_cache: {state_id_int → "NY"} for state field resolution.

    Returns:
        (total_score, breakdown)
        breakdown is a dict of {label: points} for criteria that fired,
        plus a "total" key.  Suitable for json.dumps() → x_score_breakdown.
    """
    total = 0
    breakdown: dict[str, int] = {}

    for criterion in criteria:
        field_name: str = criterion.get("field", "")
        condition_str: str = criterion.get("condition", "")
        points: int = int(criterion.get("points", 0))
        label: str = criterion.get("label", field_name)

        raw_value = lead.get(field_name)
        resolved = resolve_field_value(field_name, raw_value, state_code_cache)

        try:
            fired = evaluate_condition(condition_str, resolved)
        except Exception:
            fired = False

        if fired:
            total += points
            breakdown[label] = points

    breakdown["total"] = total
    return total, breakdown


def breakdown_to_json(breakdown: dict) -> str:
    """Serialize a score breakdown dict to a compact JSON string."""
    return json.dumps(breakdown, ensure_ascii=False)


def format_top_leads(
    scored_leads: list[tuple[dict, int, dict]],
    n: int = 10,
) -> str:
    """Format top-N scored leads as a human-readable text block.

    Args:
        scored_leads: List of (lead_dict, score, breakdown_dict) tuples,
                      already sorted descending by score.
        n:            How many to show.
    """
    lines = [f"TOP {min(n, len(scored_leads))} LEADS BY SCORE"]
    lines.append("─" * 60)

    for rank, (lead, score, breakdown) in enumerate(scored_leads[:n], start=1):
        company = lead.get("partner_name") or lead.get("name") or f"#{lead['id']}"
        city = lead.get("city") or ""
        stream = (lead.get("x_bd_stream") or "").replace("_", " ")
        stage_raw = lead.get("stage_id")
        stage = (
            stage_raw[1]
            if isinstance(stage_raw, (list, tuple)) and len(stage_raw) == 2
            else str(stage_raw or "")
        )

        location = f" ({city})" if city else ""
        lines.append(f"  #{rank:>2}  {score:>3}pt  {company}{location}  [{stream}]  stage={stage}")

        # Show what fired (skip the 'total' key)
        reasons = [
            f"    + {label} [{pts:+d}]"
            for label, pts in breakdown.items()
            if label != "total" and pts != 0
        ]
        lines.extend(reasons[:5])  # cap at 5 reasons per lead

    return "\n".join(lines)


def format_score_distribution(scores: list[int]) -> str:
    """Format a score histogram in 20-point bands."""
    bands = [(80, 100), (60, 79), (40, 59), (20, 39), (0, 19)]
    lines = ["SCORE DISTRIBUTION"]
    lines.append("─" * 30)
    for lo, hi in bands:
        count = sum(1 for s in scores if lo <= s <= hi)
        bar = "█" * min(count, 30)
        lines.append(f"  {lo:>3}–{hi}: {count:>3}  {bar}")
    return "\n".join(lines)
