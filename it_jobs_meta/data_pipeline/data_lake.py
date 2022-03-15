"""Raw data storage for job offer postings scrapped from the web."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum, auto
from pathlib import Path
from typing import Any

import boto3
import redis
from redis import Redis
from typing_extensions import Self

from it_jobs_meta.common.utils import load_yaml_as_dict


class DataLake(ABC):
    """Key-value object storage interface for raw data scraped data."""

    @abstractmethod
    def set_data(self, key: str, data: str):
        """Store data under key. Data is assumed to be json string."""

    @abstractmethod
    def get_data(self, key: str) -> str:
        """Get data stored under key. Data is assumed ot be json string."""


class RedisDataLake(DataLake):
    """Key-value object storage using Redis.

    This implementation is rather intended for development and prototyping,
    since Redis is not the main joice for data lakes.
    """

    def __init__(self, password: str, host_address: str, db_num: str):
        self._db = redis.Redis(host=host_address, password=password, db=db_num)

    @classmethod
    def from_config_file(cls, config_file_path: Path) -> Self:
        return cls(**load_yaml_as_dict(config_file_path))

    def set_data(self, key: str, data: str):
        """Store data under key. Data is assumed to be json string."""
        self._db.set(key, data)

    def get_data(self, key: str) -> str:
        """Get data stored under key. Data is assumed ot be json string."""
        data = self._db.get(key)
        if data is None:
            raise KeyError(f'No data stored in db under key: {key}')
        return data


class S3DataLake(DataLake):
    def __init__(self, bucket_name: str):
        s3 = boto3.resource("s3")
        self._bucket = s3.Bucket(bucket_name)

    @classmethod
    def from_config_file(cls, config_file_path: Path) -> Self:
        return cls(**load_yaml_as_dict(config_file_path))

    def set_data(self, key: str, data: str):
        self._bucket.put_object(Key=key, Body=data.encode('utf-8'))

    def get_data(self, key: str) -> str:
        raise NotImplemented()


class DataLakes(Enum):
    redis = auto()
    s3bucket = auto()


class DataLakeFactory:
    def __init__(self, kind: DataLakes, config_path: Path):
        self._kind = kind
        self._config_path = config_path

    def make(self):
        match self._kind:
            case DataLakes.redis:
                return RedisDataLake.from_config_file(self._config_path)
            case DataLakes.s3bucket:
                return S3DataLake.from_config_file(self._config_path)
            case _:
                raise ValueError(
                    'Selected data lake implementation is not supported or invalid'
                )
