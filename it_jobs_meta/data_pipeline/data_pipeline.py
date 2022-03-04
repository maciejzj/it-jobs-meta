import logging
from pathlib import Path

from it_jobs_meta.common.utils import setup_logging

from .data_ingestion import NoFluffJobsPostingsDataSource, PostingsDataSource
from .data_lake import DataLake, RedisDataLake, load_data_lake_db_config
from .data_warehouse import (
    EtlPipeline,
    PandasEtlExtractionFromJsonStr,
    PandasEtlNoSqlLoadingEngine,
    PandasEtlSqlLoadingEngine,
    PandasEtlTransformationEngine,
    load_warehouse_db_config,
)


def make_data_lake(data_lake_config_path: Path) -> DataLake:
    data_lake_config = load_data_lake_db_config(data_lake_config_path)
    data_lake = RedisDataLake(data_lake_config)
    return data_lake


def make_data_warehouse_etl(data_warehouse_config_path: Path) -> EtlPipeline:
    extracor = PandasEtlExtractionFromJsonStr()
    transformer = PandasEtlTransformationEngine()
    loader = PandasEtlNoSqlLoadingEngine(
        load_warehouse_db_config(data_warehouse_config_path)
    )
    data_warehouse_etl = EtlPipeline(extracor, transformer, loader)
    return data_warehouse_etl


def run_ingest_data(
    data_source: PostingsDataSource, data_lake: DataLake
) -> str:

    data = data_source.get()
    data_key = data.make_key_for_data()
    json_data_string = data.make_json_str_from_data()
    data_lake.set_data(data_key, json_data_string)
    return data_lake.get_data(data_key)


def run_data_pipeline(
    data_lake_config_path: Path, data_warehouse_config_path: Path
):
    try:
        logging.info('Started data pipeline')

        logging.info('Attempting to perform data ingestion step')
        data_source = NoFluffJobsPostingsDataSource
        data_lake = make_data_lake(data_lake_config_path)
        data = run_ingest_data(data_source, data_lake)
        logging.info('Data ingestion succeeded')

        logging.info('Attempting to perform data warehousing step')
        data_warehouse_etl = make_data_warehouse_etl(
            data_warehouse_config_path
        )
        data_warehouse_etl.run(data)
        logging.info('Data warehousing succeeded')

        logging.info('Data pipeline succeeded, exiting')
    except Exception as e:
        logging.exception(e)
        raise


def main():
    data_lake_config_path = Path('config/data_lake_db_config.yaml')
    data_warehouse_config_path = Path('config/warehouse_db_config.yaml')
    setup_logging()
    run_data_pipeline(data_lake_config_path, data_warehouse_config_path)


if __name__ == '__main__':
    main()
