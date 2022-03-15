from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from enum import Enum, auto
from typing_extensions import Self

import pymongo
import pandas as pd


from it_jobs_meta.common.utils import load_yaml_as_dict

@dataclass
class GatheredData:
    metadata: pd.DataFrame
    postings: pd.DataFrame


class DashboardDataProvider(ABC):
    @abstractmethod
    def gather_data(self) -> GatheredData:
        pass


class MongoDbDashboardDataProvider(DashboardDataProvider):
    def __init__(
        self, user_name: str, password: str, host: str, db_name: str, port=27017
    ):
        self._db_client = pymongo.MongoClient(
            f'mongodb://{user_name}:{password}@{host}:{port}'
        )
        self._db = self._db_client[db_name]

    @classmethod
    def from_config_file(cls, config_file_path: Path) -> Self:
        return cls(**load_yaml_as_dict(config_file_path))

    def gather_data(self) -> GatheredData:
        metadata_df = pd.json_normalize(self._db['metadata'].find())
        postings_df = pd.json_normalize(self._db['postings'].find())
        return GatheredData(metadata=metadata_df, postings=postings_df)


class DashboardProviders(Enum):
    MONGODB = auto()


class DashboardDataProviderFactory:
    def __init__(self, kind: DashboardProviders, config_path: Path):
        self._kind = kind
        self._config_path = config_path

    def make(self) -> DashboardDataProvider:
        match self._kind:
            case DashboardProviders.MONGODB:
                return MongoDbDashboardDataProvider.from_config_file(
                    self._config_path
                )
            case _:
                raise ValueError(
                    'Selected data provider implementation is not supported or invalid'
                )
