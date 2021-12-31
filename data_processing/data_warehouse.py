from abc import ABC, abstractmethod
import dataclasses
from dataclasses import dataclass
from pathlib import Path
from typing import Generic, TypeVar

import yaml
import pandas as pd
import pandera as pa
import sqlalchemy as db

from .data_formats import NoFluffJObsPostingsData
from .data_validation import (
    PostingsSchema, 
    SalariesSchema, 
    LocationsSchema,
    SenioritiesSchema)
from .geolocator import Geolocator


@dataclass
class EtlConstants:
    TO_DROP = [
        'renewed',
        'logo',
        'regions',
        'flavors',
        'topInSearch',
        'highlighted',
        'onlineInterviewAvailable',
        'referralBonus',
        'referralBonusCurrency']

    TO_REPLACE = {
        'node.js': 'node',
        'angular': 'javascript',
        'react': 'javascript'}

    TO_CAPITLIZE = [
        'technology',
        'category'
    ]

    POSTINGS_TABLE_COLS = [
        'name',
        'posted',
        'title',
        'technology',
        'category',
        'url',
        'remote']

    SALARIES_TABLE_COLS = [
        'contract_type',
        'salary_min',
        'salary_max',
        'salary_mean']

    LOCATIONS_TABLE_COLS = [
        'city',
        'lat',
        'lon']

    SENIORITY_TABLE_COLS = [
        'seniority']


@dataclass
class DataWarehouseDbConfig:
    protocol_name: str
    user_name: str
    password: str
    host_address: str
    db_name: str


DataType = TypeVar('DataType')
PipelineInputType = TypeVar('PipelineInputType')


class EtlExtractionEngine(Generic[PipelineInputType, DataType], ABC):
    @abstractmethod
    def extract(self, input_: PipelineInputType) -> tuple[DataType, DataType]:
        pass


class EtlTransformationEngine(Generic[DataType], ABC):
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
    def capitalize(self, data: DataType) -> DataType:
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

    @abstractmethod
    def prepare_postings_table(self, data: DataType) -> DataType:
        pass

    @abstractmethod
    def prepare_salaries_table(self, data: DataType) -> DataType:
        pass

    @abstractmethod
    def prepare_locations_table(self, data: DataType) -> DataType:
        pass

    @abstractmethod
    def prepare_seniorities_table(self, data: DataType) -> DataType:
        pass


class EtlPipeline(Generic[DataType, PipelineInputType]):
    def __init__(self,
                 extraction_engine: EtlExtractionEngine[PipelineInputType, DataType],
                 transofrmation_engine: EtlTransformationEngine[DataType],
                 loading_engine: EtlLoadingEngine[DataType]):

        self._extraction_engine = extraction_engine
        self._transformation_engine = transofrmation_engine
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
        data = self._transformation_engine.capitalize(data)
        data = self._transformation_engine.replace_values(data)
        data = self._transformation_engine.extract_remote(data)
        data = self._transformation_engine.extract_locations(data)
        data = self._transformation_engine.extract_contract_type(data)
        data = self._transformation_engine.extract_salaries(data)
        data = self._transformation_engine.unify_missing_values(data)
        return data

    def load(self, metadata: DataType, data: DataType):
        self._loading_engine.load_tables_to_warehouse(metadata, data)


def make_db_uri_from_config(config: DataWarehouseDbConfig) -> str:
    ret = (f'{config.protocol_name}://{config.user_name}:{config.password}'
           f'@{config.host_address}/{config.db_name}')
    return ret


def load_warehouse_db_config(path: Path) -> DataWarehouseDbConfig:
    with open(path, 'r', encoding='UTF-8') as cfg_file:
        cfg_dict = yaml.safe_load(cfg_file)
        return DataWarehouseDbConfig(**cfg_dict)


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
                f'"nofluffjobs", got: {data.metadata.source_name}')
        try:
            assert data.raw_data['totalCount'] == len(data.raw_data['postings'])
        except KeyError as error:
            raise ValueError(
                    'Data extractor got correct data format type and'
                    'metadata, but "raw_data" was malformed') from error


class PandasEtlTransformationEngine(EtlTransformationEngine[pd.DataFrame]):
    def __init__(self):
        self._geolocator = Geolocator()

    def drop_unwanted(self, data: pd.DataFrame) -> pd.DataFrame:
        return data.drop(columns=EtlConstants.TO_DROP)

    def drop_duplicates(self, data: pd.DataFrame) -> pd.DataFrame:
        return data[~data.index.duplicated(keep='first')]

    def replace_values(self, data: pd.DataFrame):
        return data.replace(to_replace=EtlConstants.TO_REPLACE)

    def capitalize(self, data: DataType) -> DataType:
        for column_name in EtlConstants.TO_CAPITLIZE:
            data[column_name] = data[column_name].str.capitalize()
        return data

    def extract_remote(self, data: pd.DataFrame) -> pd.DataFrame:
        data['remote'] = data['location'].transform(
            lambda location_dict: location_dict['fullyRemote'])
        return data

    def extract_locations(self, data: pd.DataFrame) -> pd.DataFrame:
        data['city'] = data['location'].transform(
            lambda location_dict: [self._geolocator(loc['city'])
                                   for loc in location_dict['places']])
        return data

    def extract_contract_type(self, data: pd.DataFrame) -> pd.DataFrame:
        data['contract_type'] = data['salary'].transform(
            lambda salary_dict: salary_dict['type'])
        return data

    def extract_salaries(self, data: pd.DataFrame) -> pd.DataFrame:
        data['salary_min'] = data['salary'].transform(
            lambda salary_dict: salary_dict['from'])
        data['salary_max'] = data['salary'].transform(
            lambda salary_dict: salary_dict['to'])
        data['salary_mean'] = data[['salary_max', 'salary_min']].mean(axis=1)
        return data

    def unify_missing_values(self, data) -> pd.DataFrame:
        return data.replace('', None)


class PandasEtlSqlLoadingEngine(EtlLoadingEngine[pd.DataFrame]):
    def __init__(self, db_config: DataWarehouseDbConfig):
        self._db_con = db.create_engine(make_db_uri_from_config(db_config))

    def load_tables_to_warehouse(self,
                                 metadata: pd.DataFrame,
                                 data: pd.DataFrame):

        metadata.to_sql('metadata', con=self._db_con, if_exists='replace')

        postings_df = self.prepare_postings_table(data)
        postings_df.to_sql('postings', con=self._db_con, if_exists='replace')

        salaries_df = self.prepare_salaries_table(data)
        salaries_df.to_sql('salaries', con=self._db_con, if_exists='replace')

        location_df = self.prepare_locations_table(data)
        location_df.to_sql('locations', con=self._db_con, if_exists='replace')

        seniority_df = self.prepare_seniorities_table(data)
        seniority_df.to_sql('seniorities', con=self._db_con, if_exists='replace')

    @pa.check_types
    def prepare_postings_table(
            self, data: pd.DataFrame) -> pa.typing.DataFrame[PostingsSchema]:
        return data[EtlConstants.POSTINGS_TABLE_COLS].reset_index()

    @pa.check_types
    def prepare_salaries_table(
            self, data: pd.DataFrame) -> pa.typing.DataFrame[SalariesSchema]:
        return data[EtlConstants.SALARIES_TABLE_COLS].reset_index()

    @pa.check_types
    def prepare_locations_table(
            self, data: pd.DataFrame) -> pa.typing.DataFrame[LocationsSchema]:
        locations_df = data.explode('city')
        locations_df[['city', 'lat', 'lon']] = locations_df['city'].transform(
            lambda city: pd.Series([city[0], city[1], city[2]]))
        locations_df = locations_df[EtlConstants.LOCATIONS_TABLE_COLS]
        return locations_df.dropna().reset_index()

    @pa.check_types
    def prepare_seniorities_table(
            self, data: pd.DataFrame) -> pa.typing.DataFrame[SenioritiesSchema]:
        seniority_df = data.explode('seniority')
        return seniority_df[EtlConstants.SENIORITY_TABLE_COLS].reset_index()
