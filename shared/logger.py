"""Shared logging setup for BD Automation Suite."""

import logging
import os
from pathlib import Path


def get_logger(name: str) -> logging.Logger:
    """Return a configured logger.

    Logs at INFO+ to stdout. If LOG_FILE env var is set, also writes to that file.
    """
    logger = logging.getLogger(name)

    if logger.handlers:
        # Already configured — return as-is to avoid duplicate handlers
        return logger

    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        "%(asctime)s [%(name)s] %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(formatter)
    logger.addHandler(console)

    log_file = os.getenv("LOG_FILE", "").strip()
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_path)
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger
