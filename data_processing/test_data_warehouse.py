import math

import pytest

from data_warehouse import (DataWarehouseDbConfig,
                            DataWarehouseETL,
                            PandasDataWarehouseETL)

POSTINGS_LIST_MOCK = [{
    'id': 'ELGZSKOL',
    'name': 'Acaisoft Poland Sp. z.o.o',
    'location': {
        'places': [
          {'country': {'code': 'POL', 'name': 'Poland'},
           'city': 'Warsaw',
           'street': '',
           'postalCode': '',
           'url': 'sql-developer-node-js-acaisoft-poland-warsaw-elgzskol'},
          {'country': {'code': 'POL', 'name': 'Poland'},
           'city': 'Gdynia',
           'street': '',
           'postalCode': '',
           'url': 'sql-developer-node-js-acaisoft-poland-gdynia-elgzskol'},
          {'country': {'code': 'POL', 'name': 'Poland'},
           'city': 'Białystok',
           'street': '',
           'postalCode': '',
           'url': 'sql-developer-node-js-acaisoft-poland-bialystok-elgzskol'}],
        'fullyRemote': True,
        'covidTimeRemotely': False},
    'posted': 1635163809168,
    'renewed': 1636895409168,
    'title': 'SQL Developer (Node.js)',
    'technology': 'sql',
    'logo': {
        'original': 'companies/logos/original/1615390311915.jpeg',
        'jobs_details': 'companies/logos/jobs_details/1615390311915.jpeg'},
    'category': 'backend',
    'seniority': ['Senior', 'Mid'],
    'url': 'sql-developer-node-js-acaisoft-poland-remote-elgzskol',
    'regions': ['pl'],
    'salary': {'from': 20000, 'to': 25000, 'type': 'b2b', 'currency': 'PLN'},
    'flavors': ['it'],
    'topInSearch': False,
    'highlighted': False,
    'onlineInterviewAvailable': True,
    'referralBonus': math.nan,
    'referralBonusCurrency': math.nan}
]

POSTINGS_RESPONSE_JSON_DICT_MOCK = {
    'postings': POSTINGS_LIST_MOCK,
    'totalCount': 1
}

POSTINGS_METADATA_DICT_MOCK = {
    'source_name': 'nofluffjobs',
    'obtained_datetime': '2021-12-01 08:30:05'
}

POSTINGS_DATA_DICT_MOCK = {
    'metadata': POSTINGS_METADATA_DICT_MOCK,
    'data': POSTINGS_RESPONSE_JSON_DICT_MOCK
}

DATA_WAREHOUSE_DB_CONFIG_MOCK = DataWarehouseDbConfig(
    protocol_name='mysql+pymysql',
    user_name='it_jobs_meta_worker',
    password='roottmppass',
    host_address='0.0.0.0',
    db_name='it_jobs_meta_datawarehouse')


class TestHappyPathPandasDataWarehouseETL:
    def setup_method(self):
        self.etl = PandasDataWarehouseETL(
            POSTINGS_DATA_DICT_MOCK, DATA_WAREHOUSE_DB_CONFIG_MOCK)

    def test_drops_unwanted_cols_correctly(self):
        self.etl.drop_unwanted()
        result = self.etl.get_processed_data_table()
        for key in DataWarehouseETL.TO_DROP:
            assert key not in result

    def test_extracts_remote_correclty(self):
        self.etl.extract_remote()
        result = self.etl.get_processed_data_table()
        assert 'remote' in result
        assert result.loc['ELGZSKOL']['remote']

    def test_extracts_locations_correctly(self):
        self.etl.extract_locations()
        result = self.etl.get_processed_data_table()
        assert 'city' in result
        assert result.loc['ELGZSKOL']['city'][0][0] == 'Warszawa'
        assert pytest.approx(result.loc['ELGZSKOL']['city'][0][1], 1) == 52.2
        assert pytest.approx(result.loc['ELGZSKOL']['city'][0][2], 1) == 21.0

    def test_extracts_contract_type_correctly(self):
        self.etl.extract_contract_type()
        result = self.etl.get_processed_data_table()
        assert 'contract_type' in result
        assert result.loc['ELGZSKOL']['contract_type'] == 'b2b'

    def test_extracts_salaries_correctly(self):
        self.etl.extract_salaries()
        result = self.etl.get_processed_data_table()
        assert 'salary_min' in result
        assert 'salary_max' in result
        assert 'salary_mean' in result
        assert result.loc['ELGZSKOL']['salary_min'] == 20000
        assert result.loc['ELGZSKOL']['salary_max'] == 25000
        assert result.loc['ELGZSKOL']['salary_mean'] == 22500
