import datetime
import json

from ..data_ingestion import NoFluffJobsPostingsDataSource


class MockResponse:
    @staticmethod
    def json():
        return {
            'postings': 'mock_postings_list',
            'totalCount': 'mock_total_count',
        }


class TestNoFluffJobsPostingsDataSource:
    def test_returns_correct_metadata_source_name(self, mocker):
        mocker.patch('requests.get', return_value=MockResponse())
        results = NoFluffJobsPostingsDataSource.get()
        source_name = results.metadata.source_name
        assert source_name == NoFluffJobsPostingsDataSource.SOURCE_NAME

    def test_returns_correct_metadata_datetime(self, mocker):
        expected = datetime.datetime(2021, 12, 1, 8, 30, 5)
        datetime_mock = mocker.patch('datetime.datetime')
        datetime_mock.now.return_value = expected

        mocker.patch('requests.get', return_value=MockResponse())

        results = NoFluffJobsPostingsDataSource.get()
        obtained_datetime = results.metadata.obtained_datetime
        assert obtained_datetime == expected

    def test_make_data_key_returns_correct_key(self, mocker):
        datetime_ = datetime.datetime(2021, 12, 1, 8, 30, 5)
        expected = '1638343805_nofluffjobs'
        datetime_mock = mocker.patch('datetime.datetime')
        datetime_mock.now.return_value = datetime_

        mocker.patch('requests.get', return_value=MockResponse())

        data = NoFluffJobsPostingsDataSource.get()
        result = data.make_key_for_data()
        assert result == expected

    def test_make_json_string_returns_json_with_correct_structure(self, mocker):
        mocker.patch('requests.get', return_value=MockResponse())

        data = NoFluffJobsPostingsDataSource.get()
        result = data.make_json_str_from_data()
        result_back_to_json_dict = json.loads(result)

        assert 'metadata' in result_back_to_json_dict.keys()
        assert 'raw_data' in result_back_to_json_dict.keys()

    def test_make_json_string_returns_correct_metadata(self, mocker):
        datetime_ = datetime.datetime(2021, 12, 1, 8, 30, 5)
        expected_json_metatada_str = {
            'source_name': 'nofluffjobs',
            'obtained_datetime': '2021-12-01 08:30:05',
        }
        datetime_mock = mocker.patch('datetime.datetime')
        datetime_mock.now.return_value = datetime_

        mocker.patch('requests.get', return_value=MockResponse())

        data = NoFluffJobsPostingsDataSource.get()
        result = data.make_json_str_from_data()
        result_back_to_json_dict = json.loads(result)
        assert (
            result_back_to_json_dict['metadata'] == expected_json_metatada_str
        )
