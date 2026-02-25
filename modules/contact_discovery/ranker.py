"""Contact candidate ranker.

Given a list of ContactCandidate objects and a priority-ordered title list,
picks the best candidate for a given BD stream.

Scoring:
  1. Title priority match — lower index in priority_titles = higher score.
     Exact match (case-insensitive) preferred; falls back to keyword overlap.
  2. Contact info bonus — +0.5 for having email, +0.25 for LinkedIn.
  3. Confidence bonus — candidate's self-reported confidence added directly.

The highest-scoring candidate wins.  Tie-breaking: prefer the one with email,
then the one with LinkedIn, then the first in input order.
"""

import re
from typing import Optional

from modules.contact_discovery.finders.base import ContactCandidate


def _title_score(candidate_title: Optional[str], priority_titles: list[str]) -> float:
    """Return a priority score for a candidate title.

    Higher is better.  Returns 0.0 if no match found.
    """
    if not candidate_title or not priority_titles:
        return 0.0

    ct = candidate_title.strip().lower()
    total = len(priority_titles)

    # 1. Exact match (case-insensitive)
    for idx, pt in enumerate(priority_titles):
        if ct == pt.strip().lower():
            return total - idx  # e.g. list of 7 → first title scores 7.0

    # 2. All words in priority title appear in candidate title
    for idx, pt in enumerate(priority_titles):
        pt_words = set(re.split(r"\W+", pt.lower())) - {""}
        ct_words = set(re.split(r"\W+", ct)) - {""}
        if pt_words and pt_words.issubset(ct_words):
            return (total - idx) * 0.8   # 80% credit for word-subset match

    # 3. Any key word from a priority title appears in candidate title
    for idx, pt in enumerate(priority_titles):
        pt_words = [w for w in re.split(r"\W+", pt.lower()) if len(w) > 3]
        if any(w in ct for w in pt_words):
            return (total - idx) * 0.4   # 40% credit for partial overlap

    return 0.0


def rank_candidates(
    candidates: list[ContactCandidate],
    priority_titles: list[str],
) -> list[ContactCandidate]:
    """Return candidates sorted best-first.

    Args:
        candidates:      All candidates found across all finders for one company.
        priority_titles: Ordered list from YAML target_titles.<stream>.priority_order.

    Returns:
        Sorted list (best candidate first).  Empty list if input is empty.
    """
    if not candidates:
        return []

    def composite_score(c: ContactCandidate) -> tuple[float, float, float, float]:
        ts = _title_score(c.title, priority_titles)
        email_bonus = 0.5 if c.email else 0.0
        li_bonus = 0.25 if c.linkedin_url else 0.0
        return (ts, c.confidence, email_bonus + li_bonus, 0.0)

    return sorted(candidates, key=composite_score, reverse=True)


def best_candidate(
    candidates: list[ContactCandidate],
    priority_titles: list[str],
) -> Optional[ContactCandidate]:
    """Return the single best ContactCandidate, or None if list is empty."""
    ranked = rank_candidates(candidates, priority_titles)
    for c in ranked:
        if c.is_actionable():
            return c
    return None
