import logging

from it_jobs_meta.data_pipeline.data_formats import NoFluffJObsPostingsData
from it_jobs_meta.data_pipeline.data_pipeline import (
    NoFluffJobsPostingsDataSource,
)
from it_jobs_meta.data_pipeline.data_warehouse import (
    make_db_uri_from_config,
    DataWarehouseDbConfig,
)
from it_jobs_meta.data_pipeline.data_lake import S3DataLake
from it_jobs_meta.data_pipeline.data_pipeline import run_ingest_data

import boto3


def lambda_handler(event, context):
    logging.info('Started data pipeline')
    logging.info('Attempting to perform data ingestion step')
    data_source = NoFluffJobsPostingsDataSource
    data_lake = S3DataLake()
    data = run_ingest_data(data_source, data_lake)
    logging.info('Data ingestion succeeded')

    return {'message': f'Hello AWS {NoFluffJObsPostingsData.__name__}'}
