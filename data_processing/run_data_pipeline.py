from dataclasses import asdict
from pathlib import Path

from data_ingestion import (
    NoFluffJobsPostingsDataSource,
    RedisDataLake,
    load_data_lake_db_config,
    make_data_key,
    make_json_string)
from data_warehouse import PandasDataWarehouseETL, load_warehouse_db_config


def main():
    data = NoFluffJobsPostingsDataSource.get()
    data_key = make_data_key(data)
    json_data_string = make_json_string(data)

    data_lake_config = load_data_lake_db_config(
        Path('data_lake_db_config.yaml'))
    data_lake = RedisDataLake(data_lake_config)
    data_lake.set(data_key, json_data_string)

    dw_config = load_warehouse_db_config(
        Path('warehouse_db_config.yaml'))
    etl = PandasDataWarehouseETL(asdict(data), dw_config)
    etl.run_etl()
    etl.load_postings_table_to_db()
    etl.load_salaries_table_to_db()
    etl.load_locations_table_to_db()
    etl.load_seniorities_table_to_db()


if __name__ == '__main__':
    main()
