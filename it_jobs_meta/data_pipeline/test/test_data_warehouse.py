import math

import pandas as pd
import pytest

from it_jobs_meta.data_pipeline.data_warehouse import (
    EtlTransformationEngine,
    PandasEtlTransformationEngine,
)


POSTINGS_LIST_MOCK = [
    {
        'id': 'ELGZSKOL',
        'name': 'Acaisoft Poland Sp. z.o.o',
        'location': {
            'places': [
                {
                    'country': {'code': 'POL', 'name': 'Poland'},
                    'city': 'Warsaw',
                    'street': '',
                    'postalCode': '',
                    'url': 'sql-developer-node-js-acaisoft-poland-warsaw-elgzskol',  # noqa: E501
                },
                {
                    'country': {'code': 'POL', 'name': 'Poland'},
                    'city': 'Gdynia',
                    'street': '',
                    'postalCode': '',
                    'url': 'sql-developer-node-js-acaisoft-poland-gdynia-elgzskol',  # noqa: E501
                },
                {
                    'country': {'code': 'POL', 'name': 'Poland'},
                    'city': 'Białystok',
                    'street': '',
                    'postalCode': '',
                    'url': 'sql-developer-node-js-acaisoft-poland-bialystok-elgzskol',  # noqa: E501
                },
            ],
            'fullyRemote': True,
            'covidTimeRemotely': False,
        },
        'posted': 1635163809168,
        'renewed': 1636895409168,
        'title': 'SQL Developer (Node.js)',
        'technology': 'sql',
        'logo': {
            'original': 'companies/logos/original/1615390311915.jpeg',
            'jobs_details': 'companies/logos/jobs_details/1615390311915.jpeg',
        },
        'category': 'backend',
        'seniority': ['Senior', 'Mid'],
        'url': 'sql-developer-node-js-acaisoft-poland-remote-elgzskol',
        'regions': ['pl'],
        'salary': {
            'from': 20000,
            'to': 25000,
            'type': 'b2b',
            'currency': 'PLN',
        },
        'flavors': ['it'],
        'topInSearch': False,
        'highlighted': False,
        'onlineInterviewAvailable': True,
        'referralBonus': math.nan,
        'referralBonusCurrency': math.nan,
    }
]

POSTINGS_RESPONSE_JSON_DICT_MOCK = {
    'postings': POSTINGS_LIST_MOCK,
    'totalCount': 1,
}

POSTINGS_METADATA_DICT_MOCK = {
    'source_name': 'nofluffjobs',
    'obtained_datetime': '2021-12-01 08:30:05',
}

POSTINGS_DATA_DICT_MOCK = {
    'metadata': POSTINGS_METADATA_DICT_MOCK,
    'data': POSTINGS_RESPONSE_JSON_DICT_MOCK,
}


class TestHappyPathPandasDataWarehouseETL:
    def setup_method(self):
        self.df = pd.DataFrame(POSTINGS_LIST_MOCK)
        self.df = self.df.set_index('id')
        self.transformer = PandasEtlTransformationEngine()

    def test_drops_unwanted_cols_correctly(self):
        result = self.transformer.drop_unwanted(self.df)
        for key in EtlTransformationEngine.COLS_TO_DROP:
            assert key not in result

    def test_extracts_remote_correctly(self):
        result = self.transformer.extract_remote(self.df)
        assert 'remote' in result
        assert result.loc['ELGZSKOL']['remote']

    def test_extracts_locations_correctly(self):
        result = self.transformer.extract_locations(self.df)
        assert 'city' in result
        assert result.loc['ELGZSKOL']['city'][0][0] == 'Warszawa'
        assert pytest.approx(result.loc['ELGZSKOL']['city'][0][1], 1) == 52.2
        assert pytest.approx(result.loc['ELGZSKOL']['city'][0][2], 1) == 21.0

    def test_extracts_contract_type_correctly(self):
        result = self.transformer.extract_contract_type(self.df)
        assert 'contract_type' in result
        assert result.loc['ELGZSKOL']['contract_type'] == 'b2b'

    def test_extracts_salaries_correctly(self):
        result = self.transformer.extract_salaries(self.df)
        assert 'salary_min' in result
        assert 'salary_max' in result
        assert 'salary_mean' in result
        assert result.loc['ELGZSKOL']['salary_min'] == 20000
        assert result.loc['ELGZSKOL']['salary_max'] == 25000
        assert result.loc['ELGZSKOL']['salary_mean'] == 22500
