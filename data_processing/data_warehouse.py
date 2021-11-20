from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict

import pandas as pd
import sqlalchemy as db

from data_ingestion import DataLake
from geolocator import Geolocator

MYSQL_ROOT_PASSWORD = 'roottmppass'
MYSQL_DATABASE = 'it_jobs_meta_datawarehouse'
MYSQL_USER = 'it_jobs_meta_worker'
MYSQL_PASSWORD = 'tmppass'


class DataWarehouseETL(ABC):
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
        'salary_mean']

    LOCATIONS_TABLE_COLS = [
        'city',
        'lat',
        'lon']

    SENIORITY_TABLE_COLS = [
        'seniority']

    def run_etl(self):
        self.drop_unwanted()
        self.drop_duplicates()
        self.replace_values()
        self.extract_remote()
        self.extract_locations()
        self.extract_contract_type()
        self.extract_salaries()

    @abstractmethod
    def drop_unwanted():
        pass

    @abstractmethod
    def replace_values():
        pass

    @abstractmethod
    def extract_remote():
        pass

    @abstractmethod
    def extract_locations():
        pass

    @abstractmethod
    def extract_contract_type():
        pass

    @abstractmethod
    def extract_salaries():
        pass

    @abstractmethod
    def get_processed_data_table():
        pass

    @abstractmethod
    def get_postings_table():
        pass

    @abstractmethod
    def get_seniority_table():
        pass

    @abstractmethod
    def get_location_table():
        pass


@dataclass
class DataWarehouseDbConfig:
    protocol_name: str
    user_name: str
    password: str
    host_address: str
    db_name: str


def make_db_uri_from_config(config: DataWarehouseDbConfig) -> str:
    ret = (f'{config.protocol_name}://{config.user_name}:{config.password}'
           f'@{config.host_address}/{config.db_name}')
    return ret


class PandasDataWarehouseETL(DataWarehouseETL):
    def __init__(self,
                 postings_data_dict: Dict[str, Any],
                 db_config: DataWarehouseDbConfig):
        self._df = pd.DataFrame.from_dict(
            postings_data_dict['data']['postings'])
        self._df.set_index('id', inplace=True)
        self._geolocator = Geolocator()
        self._db_con = db.create_engine(make_db_uri_from_config(db_config))

    def drop_unwanted(self):
        self._df.drop(columns=DataWarehouseETL.TO_DROP, inplace=True)

    def drop_duplicates(self):
        self._df.drop_duplicates(subset='index', inplace=True)

    def replace_values(self):
        self._df.replace(to_replace=DataWarehouseETL.TO_REPLACE, inplace=True)

    def extract_remote(self):
        self._df['remote'] = self._df['location'].transform(
            lambda location_dict: location_dict['fullyRemote'])

    def extract_locations(self):
        self._df['city'] = self._df['location'].transform(
            lambda loc_dict: [self._geolocator(loc['city'])
                              for loc in loc_dict['places']])

    def extract_contract_type(self):
        self._df['contract_type'] = self._df['salary'].transform(
            lambda salary_dict: salary_dict['type'])

    def extract_salaries(self):
        self._df['salary_min'] = self._df['salary'].transform(
            lambda salary_dict: salary_dict['from'])
        self._df['salary_max'] = self._df['salary'].transform(
            lambda salary_dict: salary_dict['to'])
        self._df['salary_mean'] = self._df[[
            'salary_max', 'salary_min']].mean(axis=1)

    def get_processed_data_table(self):
        return self._df

    def get_postings_table(self):
        postings_df = self._df[DataWarehouseETL.POSTINGS_TABLE_COLS]
        return postings_df

    def get_seniority_table(self):
        seniority_df = self._df.explode('seniority')
        seniority_df = seniority_df[DataWarehouseETL.SENIORITY_TABLE_COLS]
        return seniority_df

    def get_location_table(self):
        locations_df = self._df.explode('city')
        locations_df[['city', 'lat', 'lon']] = locations_df['city'].transform(
            lambda city: pd.Series([city[0], city[1], city[2]]))
        locations_df = locations_df[DataWarehouseETL.LOCATIONS_TABLE_COLS]
        locations_df.dropna(inplace=True)
        return locations_df

    def load_postings_table_to_db(self):
        postings_df = self.get_postings_table()
        postings_df.to_sql('postings', con=self._db_con, if_exists='replace')

    def load_seniority_table_to_db(self):
        seniority_df = self.get_seniority_table()
        seniority_df.to_sql('seniority', con=self._db_con, if_exists='replace')

    def load_location_table_to_db(self):
        location_df = self.get_location_table()
        location_df.to_sql('location', con=self._db_con, if_exists='replace')
