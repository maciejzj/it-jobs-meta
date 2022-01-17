"""Data containers and wrappers for data about job postings on the web."""

import dataclasses
import datetime
import json
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any


@dataclass
class PostingsMetadata:
    """Metadata to describe a batch of job postings data ingested from web."""

    source_name: str
    obtained_datetime: datetime.datetime


class PostingsData(ABC):
    """Interface for wrapper for a batch of job postings data."""

    @classmethod
    @abstractmethod
    def from_json_str(cls, json_str) -> 'PostingsData':
        """Make the data structure from json string.

        The input json should have keys:
            'metadata': Json dump of 'PostingsMetadata' with keys:
                'source_name': Name of the data source.
                'obtained_datetime': Timestamp with format 'YYYY-MM-DD HH:MM:SS'.
            'data': Unmodified scraped data in format of a json string.
        """

    @property
    @abstractmethod
    def metadata(self) -> PostingsMetadata:
        """Get the metadata associated with the data."""

    @property
    @abstractmethod
    def raw_data(self) -> Any:
        """Get the raw data scraped from the web.

        This should include data obtained 'as it was' so the return schema is
        undefined.
        """

    @abstractmethod
    def make_key_for_data(self) -> str:
        """Make unique string identifier for the scraped data batch."""

    @abstractmethod
    def make_json_str_from_data(self) -> str:
        """Get the data in form of json string.

        The returned json should have keys:
            'metadata': Json dump of 'PostingsMetadata' with keys:
                'source_name': Name of the data source.
                'obtained_datetime': Timestamp with format 'YYYY-MM-DD HH:MM:SS'.
            'data': Unmodified scraped data in format of a json string.
        """


class NoFluffJObsPostingsData(PostingsData):
    """Wrapper for a batch of postings scraped from No Fluff Jobs website."""

    def __init__(self, metadata: PostingsMetadata, raw_data: Any):
        """Create with given metadata and raw scraped data.

        Although this has no effect in the code, general program flow assumes
        that the 'raw' data is scraped from the postings REST api on No Fluff
        Jobs.
        """
        self._raw_data = raw_data
        self._metadata = metadata

    @classmethod
    def from_json_str(cls, json_str: str) -> 'NoFluffJObsPostingsData':
        """Make the data structure from json string.

        The input json should have keys:
            'metadata': Json dump of 'PostingsMetadata' with keys:
                'source_name': Name of the data source.
                'obtained_datetime': Timestamp with format 'YYYY-MM-DD HH:MM:SS'.
            'raw_data': Unmodified scraped data in format of a json string.
        """
        data_dict = json.loads(json_str)
        source_name = data_dict['metadata']['source_name']
        obtained_datetime = datetime.datetime.fromisoformat(
            data_dict['metadata']['obtained_datetime']
        )
        metadata = PostingsMetadata(source_name, obtained_datetime)
        raw_data = data_dict['raw_data']
        return cls(metadata, raw_data)

    @property
    def metadata(self) -> PostingsMetadata:
        return self._metadata

    @property
    def raw_data(self) -> Any:
        return self._raw_data

    def make_key_for_data(self) -> str:
        """Make unique string identifier for the scraped data batch.

        For this implementation it is a string of Unix epoch timestamp (with
        integer precision) and data source name (nofluffjobs).
        """
        timestamp = int(self._metadata.obtained_datetime.timestamp())
        source_name = self._metadata.source_name
        return f'{timestamp}_{source_name}'

    def make_json_str_from_data(self) -> str:
        """Get the data in form of json string.

        The returned json should have keys:
            'metadata': Json dump of 'PostingsMetadata' with keys:
                'source_name': Name of the data source.
                'obtained_datetime': Timestamp with format 'YYYY-MM-DD HH:MM:SS'.
            'raw_data': Unmodified scraped data in format of a json string. For
                No Fluff Jobs scraped data the structure is assumed to be:
                'postings': List of postings.
                'totalCount': The lenght of the list of postings.
                However, the format of the content of the 'data' is not
                guaranteed by this class.
        """
        meta_dict = dataclasses.asdict(self._metadata)
        datetime_ = meta_dict['obtained_datetime']
        meta_dict['obtained_datetime'] = datetime_.isoformat(' ', 'seconds')
        metadata_data_dict = {'metadata': meta_dict, 'raw_data': self.raw_data}
        return json.dumps(metadata_data_dict, default=str)
