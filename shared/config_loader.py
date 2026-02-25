"""YAML config loading utilities for BD Automation Suite."""

from pathlib import Path
from typing import Union

import yaml

# Config directory relative to this file's location (shared/ → project root → config/)
CONFIG_DIR = Path(__file__).resolve().parent.parent / "config"


def load_config(path: Union[str, Path]) -> dict:
    """Load and return a YAML config file as a dict.

    Args:
        path: Absolute or relative path to a YAML file.

    Raises:
        FileNotFoundError: If the file does not exist.
        yaml.YAMLError: If the file cannot be parsed as YAML.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path.resolve()}")

    try:
        with path.open("r", encoding="utf-8") as fh:
            data = yaml.safe_load(fh)
    except yaml.YAMLError as exc:
        raise yaml.YAMLError(f"Failed to parse YAML config '{path}': {exc}") from exc

    return data or {}


def get_stream_config(stream: str) -> dict:
    """Load the YAML config for a specific BD stream.

    Args:
        stream: Stream name (e.g. 'stream_a', 'stream_c').

    Raises:
        FileNotFoundError: If config/{stream}.yaml does not exist.
        yaml.YAMLError: If the file cannot be parsed.
    """
    config_path = CONFIG_DIR / f"{stream}.yaml"
    if not config_path.exists():
        raise FileNotFoundError(
            f"Stream config not found: {config_path}. "
            f"Expected file at config/{stream}.yaml"
        )
    return load_config(config_path)
