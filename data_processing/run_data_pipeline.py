import logging
import sys
from pathlib import Path
from typing import Any, Dict

from .data_ingestion import PostingsDataSource, NoFluffJobsPostingsDataSource
from .data_lake import DataLake, RedisDataLake, load_data_lake_db_config
from .data_warehouse import (
    PandasEtlSqlLoadingEngine,
    PandasEtlExtractionFromJsonStr,
    PandasEtlTransformationEngine,
    EtlPipeline,
    load_warehouse_db_config,
)


def make_data_source() -> PostingsDataSource:
    data_source = NoFluffJobsPostingsDataSource
    return data_source


def make_data_lake(data_lake_config_path: Path) -> DataLake:
    data_lake_config = load_data_lake_db_config(data_lake_config_path)
    data_lake = RedisDataLake(data_lake_config)
    return data_lake


def make_data_warehouse_etl(data_warehouse_config_path: Path) -> EtlPipeline:
    extracor = PandasEtlExtractionFromJsonStr()
    transformer = PandasEtlTransformationEngine()
    loader = PandasEtlSqlLoadingEngine(
        load_warehouse_db_config(data_warehouse_config_path)
    )
    data_warehouse_etl = EtlPipeline(extracor, transformer, loader)
    return data_warehouse_etl


def run_ingest_data(
    data_source: PostingsDataSource, data_lake: DataLake
) -> Dict[str, Any]:

    data = data_source.get()
    data_key = data.make_key_for_data()
    json_data_string = data.make_json_str_from_data()
    data_lake.set_data(data_key, json_data_string)
    return data_lake.get_data(data_key)


def run_warehouse_data(data, data_warehouse_etl):
    data_warehouse_etl.run(data)


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler("it_jobs_meta.log"),
            logging.StreamHandler(sys.stdout),
        ],
    )


def main():
    try:
        data_lake_config_path = Path(
            'data_processing/config/data_lake_db_config.yaml'
        )
        dat_warehouse_config_path = Path(
            'data_processing/config/warehouse_db_config.yaml'
        )

        setup_logging()
        logging.info('Started data pipeline')

        logging.info('Attempting to perform data ingestion step')
        data_source = make_data_source()
        data_lake = make_data_lake(data_lake_config_path)
        data = run_ingest_data(data_source, data_lake)
        logging.info('Data ingestion succeeded')

        logging.info('Attempting to perform data warehousing step')
        data_warehouse_etl = make_data_warehouse_etl(dat_warehouse_config_path)
        run_warehouse_data(data, data_warehouse_etl)
        logging.info('Data warehousing succeeded')

        logging.info('Data pipeline succeeded, exiting')
    except Exception as e:
        logging.exception(e)
        raise


if __name__ == '__main__':
    main()
