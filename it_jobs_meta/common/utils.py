"""Utility tools shared across the application."""

import logging
import sys
from pathlib import Path
from typing import Any

import yaml


def setup_logging(*args: Path, log_level: int = logging.INFO):
    """Enable logging to stdout and the given files.

    :param *args: Paths to log output files.
    """
    log_file_handlers = []
    for log_path in args:
        log_path.parent.mkdir(exist_ok=True, parents=True)
        log_file_handlers.append(logging.FileHandler(log_path))

    logging.basicConfig(
        level=log_level,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[
            *log_file_handlers,
            logging.StreamHandler(sys.stdout),
        ],
    )


def load_yaml_as_dict(path: Path) -> dict[str, Any]:
    with open(path, 'r', encoding='UTF-8') as yaml_file:
        return yaml.safe_load(yaml_file)
