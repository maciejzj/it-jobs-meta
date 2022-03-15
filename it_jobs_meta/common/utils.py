import logging
import sys
from pathlib import Path
from typing import Any

import yaml


def setup_logging(log_path: Path = Path()):
    log_path.parent.mkdir(exist_ok=True, parents=True)
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[
            logging.FileHandler(log_path),
            logging.StreamHandler(sys.stdout),
        ],
    )


def load_yaml_as_dict(path: Path) -> dict[str, Any]:
    with open(path, 'r', encoding='UTF-8') as yaml_file:
        return yaml.safe_load(yaml_file)
