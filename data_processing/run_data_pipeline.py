from dataclasses import asdict
from pathlib import Path
import logging
import sys

from data_ingestion import (
    PostingsDataSource,
    NoFluffJobsPostingsDataSource,
    DataLake,
    RedisDataLake,
    load_data_lake_db_config,
    make_data_key,
    make_json_string)
from data_warehouse import (
    DataWarehouseETL,
    PandasDataWarehouseETL,
    load_warehouse_db_config)


def make_data_source() -> PostingsDataSource:
    data_source = NoFluffJobsPostingsDataSource
    return data_source


def make_data_lake() -> DataLake:
    data_lake_config = load_data_lake_db_config(
        Path('data_processing/data_lake_db_config.yaml'))
    data_lake = RedisDataLake(data_lake_config)
    return data_lake


def make_data_warehouse_etl() -> DataWarehouseETL:
    dw_config = load_warehouse_db_config(
        Path('data_processing/warehouse_db_config.yaml'))
    data_warehouse_etl = PandasDataWarehouseETL(dw_config)
    return data_warehouse_etl


def run_ingest_data(data_source: PostingsDataSource,
                    data_lake: DataLake):
    data = data_source.get()
    data_key = make_data_key(data)
    json_data_string = make_json_string(data)

    data_lake.set(data_key, json_data_string)
    return asdict(data)


def run_warehouse_data(data, data_warehouse_etl: DataWarehouseETL):
    data_warehouse_etl.run_etl(data)


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler("it_jobs_meta.log"),
            logging.StreamHandler(sys.stdout)])


def main():
    try:
        setup_logging()
        logging.info('Started data pipeline')

        logging.info('Attempting to perform data ingestion step')
        data_source = make_data_source()
        data_lake = make_data_lake()
        data = run_ingest_data(data_source, data_lake)
        logging.info('Data ingestion succeeded')

        logging.info('Attempting to perform data warehousing step')
        data_warehouse_etl = make_data_warehouse_etl()
        run_warehouse_data(data, data_warehouse_etl)
        logging.info('Data warehousing succeeded')

        logging.info('Data pipeline succeeded, exiting')
    except Exception as e:
        logging.exception(e)
        raise


if __name__ == '__main__':
    main()
