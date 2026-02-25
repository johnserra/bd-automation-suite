"""Base types for all contact discovery finders."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import ClassVar, Optional


@dataclass
class ContactCandidate:
    """A potential decision-maker contact found by a finder.

    Multiple candidates may be returned per company; ranker.py selects the
    best one based on title priority.
    """

    name: str
    title: Optional[str] = None
    email: Optional[str] = None
    linkedin_url: Optional[str] = None
    phone: Optional[str] = None
    confidence: float = 0.0       # 0.0–1.0 (finder's self-assessment)
    source: str = ""              # "website", "hunter_io", "linkedin_manual"
    raw: dict = field(default_factory=dict)

    def is_actionable(self) -> bool:
        """True if the candidate has at least a name (the minimum to write back)."""
        return bool(self.name and self.name.strip())

    def has_contact_info(self) -> bool:
        """True if the candidate has email or LinkedIn (can act immediately)."""
        return bool(self.email or self.linkedin_url)


class BaseContactFinder(ABC):
    """Abstract interface for all contact finders."""

    name: ClassVar[str] = "base"

    @abstractmethod
    def find(
        self,
        lead: dict,
        priority_titles: list[str],
        finder_config: dict,
    ) -> list[ContactCandidate]:
        """Search for contacts at the company described by lead.

        Args:
            lead:            Odoo lead dict (partner_name, website, city, etc.).
            priority_titles: Ordered list of desired job titles from YAML.
            finder_config:   The dict under search_methods.<name> in the YAML.

        Returns:
            List of ContactCandidate objects (may be empty). Should NOT raise —
            catch exceptions internally and return what was found.
        """
        ...

    def is_enabled(self, search_methods_config: dict) -> bool:
        return bool(search_methods_config.get(self.name, {}).get("enabled", False))

    def get_config(self, search_methods_config: dict) -> dict:
        return search_methods_config.get(self.name, {})
