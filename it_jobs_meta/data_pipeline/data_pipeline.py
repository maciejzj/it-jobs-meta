import datetime as dt
import logging
from pathlib import Path
from time import sleep
from typing import Optional

import croniter

from it_jobs_meta.common.utils import setup_logging
from it_jobs_meta.data_pipeline.data_lake import DataLakeFactory
from it_jobs_meta.data_pipeline.data_warehouse import (
    EtlLoaderFactory,
    EtlPipeline,
    PandasEtlExtractionFromJsonStr,
    PandasEtlTransformationEngine,
)

from .data_ingestion import NoFluffJobsPostingsDataSource


class DataPipeline:
    def __init__(
        self,
        data_lake_factory: DataLakeFactory,
        etl_loader_factory: EtlLoaderFactory,
    ):
        self._data_lake_factory = data_lake_factory
        self._etl_loader_factory = etl_loader_factory

    def schedule(self, cron_expression: Optional[str] = None):
        logging.info(
            f'Data pipeline scheduled with cron expression: "{cron_expression}", (if None, will run once) send SIGINT to stop'
        )
        self.run()

        if cron_expression is not None:
            now = dt.datetime.now()
            cron = croniter.croniter(cron_expression, now)

            try:
                while True:
                    self.run()
                    now = dt.datetime.now()
                    timedelta_till_next_trigger = (
                        cron.get_next(dt.datetime) - now
                    )
                    sleep(timedelta_till_next_trigger.total_seconds())
            except KeyboardInterrupt:
                logging.info(f'Data pipeline loop interrupted by user`')

    def run(self):
        try:
            logging.info('Started data pipeline')

            logging.info('Attempting to perform data ingestion step')
            data = NoFluffJobsPostingsDataSource.get()
            logging.info('Data ingestion succeeded')

            logging.info('Attempting to archive raw data in data lake')
            data_lake = self._data_lake_factory.make()

            data_key = data.make_key_for_data()
            data_as_json = data.make_json_str_from_data()
            data_lake.set_data(data_key, data.make_json_str_from_data())
            logging.info(
                f'Data archival succeeded, stored under "{data_key}" key'
            )

            logging.info('Attempting to perform data warehousing step')
            data_warehouse_etl = EtlPipeline(
                PandasEtlExtractionFromJsonStr(),
                PandasEtlTransformationEngine(),
                self._etl_loader_factory.make(),
            )
            data_warehouse_etl.run(data_as_json)
            logging.info('Data warehousing succeeded')

            logging.info('Data pipeline succeeded')
        except Exception as e:
            logging.exception(e)
            raise


def main():
    data_lake_config_path = Path('config/data_lake_db_config.yaml')
    data_warehouse_config_path = Path('config/warehouse_db_config.yaml')
    setup_logging()


if __name__ == '__main__':
    main()
