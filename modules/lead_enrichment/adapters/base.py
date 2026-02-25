"""Base types for all lead enrichment adapters."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import ClassVar


@dataclass
class EnrichmentResult:
    """Result returned by a single enrichment adapter for one lead.

    fields_updated: Odoo field → value pairs ready to write.
    description_note: Text to append to the lead's description field.
    error: Non-empty when success=False; explains why the adapter skipped/failed.
    """

    source: str
    success: bool
    fields_updated: dict = field(default_factory=dict)
    description_note: str = ""
    error: str = ""


class BaseEnrichmentAdapter(ABC):
    """Abstract interface for all lead enrichment adapters."""

    name: ClassVar[str] = "base"

    @abstractmethod
    def enrich(
        self,
        lead: dict,
        fields_to_update: list[str],
        adapter_config: dict,
    ) -> EnrichmentResult:
        """Enrich a lead from this adapter's data source.

        Args:
            lead:             Odoo lead dict (partner_name, website, city, etc.).
            fields_to_update: Which Odoo fields this adapter should populate.
            adapter_config:   The full enrichment YAML config dict (for global settings).

        Returns:
            EnrichmentResult — always returns, never raises.
        """
        ...

    def is_source_configured(self, source_list: list[dict]) -> bool:
        """True if this adapter's source name appears in the stream's source list."""
        return any(s.get("source") == self.name for s in source_list)

    def get_fields_to_update(self, source_list: list[dict]) -> list[str]:
        """Return the fields_to_update list for this adapter from the stream config."""
        for s in source_list:
            if s.get("source") == self.name:
                return s.get("fields_to_update", [])
        return []
