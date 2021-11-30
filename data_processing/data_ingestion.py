import dataclasses
import datetime
import json
import requests
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import redis
import yaml


@dataclass
class PostingsMetadata:
    source_name: str
    obtained_datetime: datetime


@dataclass
class PostingsData:
    metadata: PostingsMetadata
    data: Any


class PostingsDataSource(ABC):
    @abstractmethod
    def get() -> PostingsData:
        pass


class DataLake(ABC):
    @abstractmethod
    def set(self, key: str, data: str):
        pass

    @abstractmethod
    def get(self, key: str) -> str:
        pass


def make_data_key(dataset: PostingsData) -> str:
    timestamp = int(dataset.metadata.obtained_datetime.timestamp())
    source_name = dataset.metadata.source_name
    return f'{timestamp}_{source_name}'


def make_json_string(dataset: PostingsData) -> str:
    data_dict = dataclasses.asdict(dataset)
    json_string = json.dumps(data_dict, default=str)
    return json_string


class NoFluffJobsPostingsDataSource(PostingsDataSource):
    POSTINGS_API_URL_SOURCE = 'https://nofluffjobs.com/api/posting'
    SOURCE_NAME = 'nofluffjobs'

    @classmethod
    def get(cls) -> PostingsData:
        r = requests.get(cls.POSTINGS_API_URL_SOURCE)
        json_data = r.json()
        datetime_now = datetime.datetime.now()

        metadata = PostingsMetadata(
            source_name=cls.SOURCE_NAME,
            obtained_datetime=datetime_now)
        data = PostingsData(
            metadata=metadata,
            data=json_data)

        return data


@dataclass
class DataLakeDbConfig:
    password: str
    host_address: str
    db_num: int


def load_data_lake_db_config(path: Path) -> DataLakeDbConfig:
    with open(path, 'r') as cfg_file:
        cfg_dict = yaml.safe_load(cfg_file)
        return DataLakeDbConfig(**cfg_dict)


class RedisDataLake(DataLake):
    def __init__(self, db_config: DataLakeDbConfig):
        self._db = redis.Redis(host=db_config.host_address,
                               password=db_config.password,
                               db=db_config.db_num)

    def set(self, key: str, data: str):
        self._db.set(key, data)

    def get(self, key: str) -> dict[str, Any]:
        return json.loads(self._db.get(key))


def main():
    data = NoFluffJobsPostingsDataSource.get()
    data_lake = RedisDataLake('0.0.0.0', 6379, 0)
    data_lake.set(make_data_key(data), make_json_string(data))


if __name__ == '__main__':
    main()
