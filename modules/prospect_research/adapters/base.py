"""Abstract base class for all Prospect Research adapters.

Each data source (trade data service, Google Maps, etc.) implements this interface.
The orchestrator calls fetch() and gets a list of ProspectRecords back,
regardless of how the source was queried.
"""

from abc import ABC, abstractmethod
from typing import ClassVar

from modules.prospect_research.normalizer import ProspectRecord


class BaseAdapter(ABC):
    """Abstract adapter interface.

    Subclasses must set the `name` class variable to match the key used in
    the data_sources section of the stream YAML config.  Example:

        class TradeDataAdapter(BaseAdapter):
            name = "trade_data"

    Then in stream_c.yaml:
        data_sources:
          trade_data:
            enabled: true
            hs_codes: [...]
    """

    name: ClassVar[str] = "base"

    @abstractmethod
    def fetch(
        self,
        adapter_config: dict,
        stream: str,
        profile: dict,
    ) -> list[ProspectRecord]:
        """Fetch prospect records from the data source.

        Args:
            adapter_config: The dict under data_sources.<name> in the YAML.
                            Guaranteed to have enabled=True when called.
            stream:         BD stream key, e.g. 'stream_a'.
            profile:        The target_profile dict from the YAML.

        Returns:
            List of ProspectRecord objects.  May be empty if nothing found
            or all requests fail.  Should NOT raise — log errors and return
            what was successfully collected.
        """
        ...

    def is_enabled(self, data_sources_config: dict) -> bool:
        """Return True if this adapter is enabled in the stream config."""
        return bool(
            data_sources_config.get(self.name, {}).get("enabled", False)
        )

    def get_adapter_config(self, data_sources_config: dict) -> dict:
        """Return the adapter-specific config dict (may be empty)."""
        return data_sources_config.get(self.name, {})
