from abc import ABC, abstractmethod
from typing import Any, Dict

import pandas as pd

from data_ingestion import DataLake
from geolocator import Geolocator


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
        'id',
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
        'id',
        'city',
        'lat',
        'lon']

    SENIORITY_TABLE_COLS = [
        'id',
        'seniority']

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


class PandasDataWarehouseETL(DataWarehouseETL):
    def __init__(self, postings_data_dict: Dict[str, Any]):
        self._df = pd.DataFrame.from_dict(
            postings_data_dict['data']['postings'])
        self._df.set_index('id', inplace=True)
        self._geolocator = Geolocator()

    def drop_unwanted(self):
        self._df.drop(columns=DataWarehouseETL.TO_DROP, inplace=True)

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
        postings_df.drop_duplicates(subset='id', inplace=True)
        return postings_df

    def get_seniority_table(self):
        seniority_df = self._df[DataWarehouseETL.SENIORITY_TABLE_COLS]
        seniority_df.explode('seniority', inplace=True)
        return seniority_df

    def get_location_table(self):
        locations_df = self._df[DataWarehouseETL.LOCATIONS_TABLE_COLS]
        locations_df.explode('city', inplace=True)
        locations_df['city'] = locations_df['city'].transform(
            lambda location_list: location_list[0])
        locations_df['lat'] = locations_df['city'].transform(
            lambda location_list: location_list[1])
        locations_df['lon'] = locations_df['city'].transform(
            lambda location_list: location_list[2])
        locations_df.drop(columns='location')
        locations_df.dropna(inplace=True)
        return locations_df
