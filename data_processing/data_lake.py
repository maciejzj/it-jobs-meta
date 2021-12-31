"""Raw data storage for job offer postings scrapped from the web."""

import json
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import redis
import yaml


@dataclass
class DataLakeDbConfig:
    password: str
    host_address: str
    db_num: int


class DataLake(ABC):
    """Key-value object storage interface for raw data scraped data."""

    @abstractmethod
    def set_data(self, key: str, data: str):
        """Store data under key. Data is assumed to be json string."""

    @abstractmethod
    def get_data(self, key: str) -> dict[str, Any]:
        """Get data stored under key. Data is assumed ot be json string."""


def load_data_lake_db_config(path: Path) -> DataLakeDbConfig:
    with open(path, 'r', encoding='UTF-8') as cfg_file:
        cfg_dict = yaml.safe_load(cfg_file)
        return DataLakeDbConfig(**cfg_dict)


class RedisDataLake(DataLake):
    """Key-value object storage using Redis.

    This implementation is rather intended for development and prototyping,
    since Redis is not the main joice for data lakes.
    """
    def __init__(self, db_config: DataLakeDbConfig):
        self._db = redis.Redis(host=db_config.host_address,
                               password=db_config.password,
                               db=db_config.db_num)

    def set_data(self, key: str, data: str):
        """Store data under key. Data is assumed to be json string."""
        self._db.set(key, data)

    def get_data(self, key: str) -> str:
        """Get data stored under key. Data is assumed ot be json string."""
        data = self._db.get(key)
        if data is None:
            raise KeyError(f'No data stored in db under key: {key}')
        return data
