"""Run the data pipeline or dashboard using CLI options."""

from it_jobs_meta.common.cli import CliArgumentParser
from it_jobs_meta.common.utils import setup_logging
from it_jobs_meta.dashboard.dashboard import (
    DashboardApp,
    DashboardDataProviderFactory,
)
from it_jobs_meta.data_pipeline.data_lake import DataLakeFactory
from it_jobs_meta.data_pipeline.data_pipeline import DataPipeline
from it_jobs_meta.data_pipeline.data_etl import EtlLoaderFactory


def main():
    parser = CliArgumentParser()
    setup_logging(parser.args['log_path'])

    match parser.args['command']:
        case 'pipeline':
            data_lake_type, data_lake_cfg_path = parser.extract_data_lake()
            (
                warehouse_type,
                warehouse_cfg_path,
            ) = parser.extract_etl_loader()
            data_lake_factory = DataLakeFactory(
                data_lake_type, data_lake_cfg_path
            )
            etl_loader_factory = EtlLoaderFactory(
                warehouse_type, warehouse_cfg_path
            )

            data_pipeline = DataPipeline(
                data_lake_factory,
                etl_loader_factory,
            )

            if parser.args['schedule'] is not None:
                data_pipeline.schedule(parser.args['schedule'])
            else:
                data_pipeline.run()

        case 'dashboard':
            provider_type, provider_cfg_path = parser.extract_data_provider()
            etl_loader_factory = DashboardDataProviderFactory(
                provider_type, provider_cfg_path
            )
            app = DashboardApp(etl_loader_factory)
            app.run(parser.args['with_wsgi'])


if __name__ == '__main__':
    main()
