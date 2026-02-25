"""Shared infrastructure for BD Automation Suite."""

from shared.config_loader import load_config, get_stream_config
from shared.logger import get_logger
from shared.llm_client import LLMClient
from shared.odoo_client import OdooClient

__all__ = ["OdooClient", "LLMClient", "get_logger", "load_config", "get_stream_config"]
