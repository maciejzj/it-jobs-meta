"""Job postings data ingestion and web scraping."""

import datetime as dt
from abc import ABC, abstractmethod

import requests

from it_jobs_meta.data_pipeline.data_formats import (
    NoFluffJObsPostingsData,
    PostingsData,
    PostingsMetadata,
)


class PostingsDataSource(ABC):
    """Source for fetching postings data from the web."""

    @classmethod
    @abstractmethod
    def get(cls) -> PostingsData:
        """Get a batch of postings data from the source."""


class NoFluffJobsPostingsDataSource(PostingsDataSource):
    """Source of postings data scraped using REST api from No Fluff Jobs."""

    POSTINGS_API_URL_SOURCE = 'https://nofluffjobs.com/api/posting'
    SOURCE_NAME = 'nofluffjobs'

    @classmethod
    def get(cls) -> PostingsData:
        """Get a snapshot of postings data from No Fluff Jobs in one batch."""
        response = requests.get(cls.POSTINGS_API_URL_SOURCE)
        raw_data = response.json()
        datetime_now = dt.datetime.now()

        metadata = PostingsMetadata(
            source_name=cls.SOURCE_NAME, obtained_datetime=datetime_now
        )
        data = NoFluffJObsPostingsData(metadata=metadata, raw_data=raw_data)

        return data


def main():
    """Demo main function for ad-hock tests.

    Gets the current postings from No Fluff Jobs and prints them as a json
    dict alongside the metadata.
    """
    data = NoFluffJobsPostingsDataSource.get()
    print(data.make_json_str_from_data())


if __name__ == '__main__':
    main()
