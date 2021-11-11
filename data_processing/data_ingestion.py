import datetime
import logging
import requests
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any


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
    def insert(key: str, data: Any):
        pass

    @abstractmethod
    def get(key: str) -> Any:
        pass


def make_key_for_data(dataset: PostingsData) -> str:
    timestamp = dataset.metadata.obtained_datetime.timestamp()
    source_name = dataset.metadata.source_name
    return f'{timestamp}_{source_name}'


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

        logging.info(
            f'Scraped new data from {cls.SOURCE_NAME}, on {datetime_now}.')
        return data
