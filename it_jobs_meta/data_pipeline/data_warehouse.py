import dataclasses
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from time import clock_settime
from typing import Generic, TypeVar
from enum import Enum, auto

import pandas as pd
import pymongo
import sqlalchemy as db
import yaml
from typing_extensions import Self

from it_jobs_meta.common.utils import load_yaml_as_dict

from .data_formats import NoFluffJObsPostingsData
from .data_validation import Schemas
from .geolocator import Geolocator

DataType = TypeVar('DataType')
PipelineInputType = TypeVar('PipelineInputType')


class EtlExtractionEngine(Generic[PipelineInputType, DataType], ABC):
    @abstractmethod
    def extract(self, input_: PipelineInputType) -> tuple[DataType, DataType]:
        pass


class EtlTransformationEngine(Generic[DataType], ABC):
    COLS_TO_DROP = [
        'renewed',
        'logo',
        'regions',
        'flavors',
        'topInSearch',
        'highlighted',
        'onlineInterviewAvailable',
        'referralBonus',
        'referralBonusCurrency',
    ]

    VALS_TO_REPLACE = {
        'node.js': 'node',
        'angular': 'javascript',
        'react': 'javascript',
    }

    COLS_TO_TITLE_CASE = ['category']

    COLS_TO_CAPITALIZE = ['technology', 'contract_type']

    CAPITALIZE_SPECIAL_NAMES = {
        '.net': '.Net',
        'aws': 'AWS',
        'ios': 'iOS',
        'javascript': 'JavaScript',
        'php': 'PHP',
        'sql': 'SQL',
        'b2b': 'B2B',
    }

    @abstractmethod
    def drop_unwanted(self, data: DataType) -> DataType:
        pass

    @abstractmethod
    def drop_duplicates(self, data: DataType) -> DataType:
        pass

    @abstractmethod
    def replace_values(self, data: DataType) -> DataType:
        pass

    @abstractmethod
    def to_title_case(self, data: DataType) -> DataType:
        pass

    @abstractmethod
    def to_capitalized(self, data: DataType) -> DataType:
        pass

    @abstractmethod
    def extract_remote(self, data: DataType) -> DataType:
        pass

    @abstractmethod
    def extract_locations(self, data: DataType) -> DataType:
        pass

    @abstractmethod
    def extract_contract_type(self, data: DataType) -> DataType:
        pass

    @abstractmethod
    def extract_salaries(self, data: DataType) -> DataType:
        pass

    @abstractmethod
    def unify_missing_values(self, data: DataType) -> DataType:
        pass


class EtlLoadingEngine(Generic[DataType], ABC):
    @abstractmethod
    def load_tables_to_warehouse(self, metadata: DataType, data: DataType):
        pass


class EtlPipeline(Generic[DataType, PipelineInputType]):
    def __init__(
        self,
        extraction_engine: EtlExtractionEngine[PipelineInputType, DataType],
        transformation_engine: EtlTransformationEngine[DataType],
        loading_engine: EtlLoadingEngine[DataType],
    ):

        self._extraction_engine = extraction_engine
        self._transformation_engine = transformation_engine
        self._loading_engine = loading_engine

    def run(self, input_: PipelineInputType):
        metadata, data = self.extract(input_)
        data = self.transform(data)
        self.load(metadata, data)

    def extract(self, input_: PipelineInputType) -> tuple[DataType, DataType]:
        return self._extraction_engine.extract(input_)

    def transform(self, data: DataType) -> DataType:
        data = self._transformation_engine.drop_unwanted(data)
        data = self._transformation_engine.drop_duplicates(data)
        data = self._transformation_engine.extract_remote(data)
        data = self._transformation_engine.extract_locations(data)
        data = self._transformation_engine.extract_contract_type(data)
        data = self._transformation_engine.extract_salaries(data)
        data = self._transformation_engine.replace_values(data)
        data = self._transformation_engine.to_title_case(data)
        data = self._transformation_engine.to_capitalized(data)
        data = self._transformation_engine.unify_missing_values(data)
        return data

    def load(self, metadata: DataType, data: DataType):
        self._loading_engine.load_tables_to_warehouse(metadata, data)


class PandasEtlExtractionFromJsonStr(EtlExtractionEngine[str, pd.DataFrame]):
    def __init__(self):
        pass

    def extract(self, input_: str) -> tuple[pd.DataFrame, pd.DataFrame]:
        data = NoFluffJObsPostingsData.from_json_str(input_)
        self.validate_nofluffjobs_data(data)
        metadata_df = pd.DataFrame(dataclasses.asdict(data.metadata), index=[0])
        data_df = pd.DataFrame(data.raw_data['postings'])
        data_df = data_df.set_index('id')
        return metadata_df, data_df

    @staticmethod
    def validate_nofluffjobs_data(data: NoFluffJObsPostingsData):
        if data.metadata.source_name != 'nofluffjobs':
            raise ValueError(
                'Data extractor got correct data format type, but the '
                'metadata indicates invlaid data source; expected: '
                f'"nofluffjobs", got: {data.metadata.source_name}'
            )
        try:
            assert data.raw_data['totalCount'] == len(data.raw_data['postings'])
        except KeyError as error:
            raise ValueError(
                'Data extractor got correct data format type and'
                'metadata, but "raw_data" was malformed'
            ) from error


class PandasEtlTransformationEngine(EtlTransformationEngine[pd.DataFrame]):
    def __init__(self):
        self._geolocator = Geolocator()

    def drop_unwanted(self, data: pd.DataFrame) -> pd.DataFrame:
        return data.drop(columns=EtlTransformationEngine.COLS_TO_DROP)

    def drop_duplicates(self, data: pd.DataFrame) -> pd.DataFrame:
        return data[~data.index.duplicated(keep='first')]

    def replace_values(self, data: pd.DataFrame) -> pd.DataFrame:
        return data.replace(to_replace=EtlTransformationEngine.VALS_TO_REPLACE)

    def to_title_case(self, data: pd.DataFrame) -> pd.DataFrame:
        for col in EtlTransformationEngine.COLS_TO_TITLE_CASE:
            data[col] = data[col][data[col].notna()].transform(
                lambda s: re.sub(r'([A-Z])', r' \1', s).title()
            )
        return data

    def to_capitalized(self, data: pd.DataFrame) -> pd.DataFrame:
        specials = EtlTransformationEngine.CAPITALIZE_SPECIAL_NAMES
        for col in EtlTransformationEngine.COLS_TO_CAPITALIZE:
            data[col] = data[col][data[col].notna()].transform(
                lambda s: specials[s] if s in specials else s.capitalize()
            )
        return data

    def extract_remote(self, data: pd.DataFrame) -> pd.DataFrame:
        data['remote'] = data['location'].transform(
            lambda location_dict: location_dict['fullyRemote']
        )
        return data

    def extract_locations(self, data: pd.DataFrame) -> pd.DataFrame:
        data['city'] = data['location'].transform(
            lambda location_dict: [
                self._geolocator(loc['city']) for loc in location_dict['places']
            ]
        )
        return data

    def extract_contract_type(self, data: pd.DataFrame) -> pd.DataFrame:
        data['contract_type'] = data['salary'].transform(
            lambda salary_dict: salary_dict['type']
        )
        return data

    def extract_salaries(self, data: pd.DataFrame) -> pd.DataFrame:
        data['salary_min'] = data['salary'].transform(
            lambda salary_dict: salary_dict['from']
        )
        data['salary_max'] = data['salary'].transform(
            lambda salary_dict: salary_dict['to']
        )
        data['salary_mean'] = data[['salary_max', 'salary_min']].mean(axis=1)

        data = data[
            data['salary'].transform(
                lambda salary_dict: salary_dict['currency'] == 'PLN'
            )
        ]
        return data

    def unify_missing_values(self, data: pd.DataFrame) -> pd.DataFrame:
        data = data.replace('', None)
        data = data.replace('NaN', None)
        data = data.replace('Nan', None)
        data = data.replace('nan', None)
        return data


class PandasEtlNoSqlLoadingEngine(EtlLoadingEngine[pd.DataFrame]):
    POSTINGS_TABLE_COLS = [
        'name',
        'posted',
        'title',
        'technology',
        'category',
        'url',
        'remote',
        'contract_type',
        'salary_min',
        'salary_max',
        'salary_mean',
        'city',
        'seniority',
    ]

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

    def load_tables_to_warehouse(
        self, metadata: pd.DataFrame, data: pd.DataFrame
    ):
        self._db['metadata'].drop()
        self._db['postings'].drop()

        self._db['metadata'].insert_one(metadata.to_dict('records')[0])
        self._db['postings'].insert_many(
            data[PandasEtlNoSqlLoadingEngine.POSTINGS_TABLE_COLS].to_dict(
                'records'
            )
        )


class PandasEtlSqlLoadingEngine(EtlLoadingEngine[pd.DataFrame]):
    POSTINGS_TABLE_COLS = [
        'name',
        'posted',
        'title',
        'technology',
        'category',
        'url',
        'remote',
    ]

    SALARIES_TABLE_COLS = [
        'contract_type',
        'salary_min',
        'salary_max',
        'salary_mean',
    ]

    LOCATIONS_TABLE_COLS = ['city', 'lat', 'lon']

    SENIORITY_TABLE_COLS = ['seniority']

    def __init__(
        self, user_name: str, password: str, host: str, db_name: str, port=3306
    ):
        self._db_con = db.create_engine(
            f'mysql+pymysql://{user_name}:{password}@{host}:{port}/{db_name}'
        )

    @classmethod
    def from_config_file(cls, config_file_path: Path) -> Self:
        return cls(**load_yaml_as_dict(config_file_path))

    def load_tables_to_warehouse(
        self, metadata: pd.DataFrame, data: pd.DataFrame
    ):
        metadata.to_sql('metadata', con=self._db_con, if_exists='replace')

        postings_df = self.prepare_postings_table(data)
        postings_df.to_sql('postings', con=self._db_con, if_exists='replace')

        salaries_df = self.prepare_salaries_table(data)
        salaries_df.to_sql('salaries', con=self._db_con, if_exists='replace')

        location_df = self.prepare_locations_table(data)
        location_df.to_sql('locations', con=self._db_con, if_exists='replace')

        seniority_df = self.prepare_seniorities_table(data)
        seniority_df.to_sql(
            'seniorities', con=self._db_con, if_exists='replace'
        )

    def prepare_postings_table(self, data: pd.DataFrame) -> pd.DataFrame:
        postings_df = data[
            PandasEtlSqlLoadingEngine.POSTINGS_TABLE_COLS
        ].reset_index()
        return Schemas.postings.validate(postings_df)

    def prepare_salaries_table(self, data: pd.DataFrame) -> pd.DataFrame:
        salaries_df = data[
            PandasEtlSqlLoadingEngine.SALARIES_TABLE_COLS
        ].reset_index()
        return Schemas.salaries.validate(salaries_df)

    def prepare_locations_table(self, data: pd.DataFrame) -> pd.DataFrame:
        locations_df = data.explode('city')
        locations_df[['city', 'lat', 'lon']] = locations_df['city'].transform(
            lambda city: pd.Series([city[0], city[1], city[2]])
        )
        locations_df = locations_df[
            PandasEtlSqlLoadingEngine.LOCATIONS_TABLE_COLS
        ]
        locations_df = locations_df.dropna().reset_index()
        return Schemas.locations.validate(locations_df)

    def prepare_seniorities_table(self, data: pd.DataFrame) -> pd.DataFrame:
        seniority_df = data.explode('seniority')
        seniority_df = seniority_df[
            PandasEtlSqlLoadingEngine.SENIORITY_TABLE_COLS
        ].reset_index()
        return Schemas.seniorities.validate(seniority_df)


class EtlLoaders(Enum):
    MONGODB = auto()
    SQL = auto()


class EtlLoaderFactory:
    def __init__(self, kind: EtlLoaders, config_path: Path):
        self._kind = kind
        self._config_path = config_path

    def make(self):
        match self._kind:
            case EtlLoaders.MONGODB:
                return PandasEtlNoSqlLoadingEngine.from_config_file(
                    self._config_path
                )
            case EtlLoaders.SQL:
                return PandasEtlSqlLoadingEngine.from_config_file(
                    self._config_path
                )
            case _:
                raise ValueError(
                    'Selected data lake implementation is not supported or invalid'
                )
