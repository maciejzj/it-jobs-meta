import argparse
from pathlib import Path
from typing import Any

from it_jobs_meta.dashboard.dashboard import DashboardProviderImpl
from it_jobs_meta.data_pipeline.data_lake import DataLakeImpl
from it_jobs_meta.data_pipeline.data_warehouse import EtlLoaderImpl


class CliArgumentParser:
    PROG = 'it-jobs-meta'
    DESCRIPTION = (
        'Data pipeline and meta-analysis dashboard for IT job postings'
    )

    def __init__(self):
        self._args: dict[str, Any] | None = None
        self._parser = argparse.ArgumentParser(
            prog=self.PROG, description=self.DESCRIPTION
        )
        self._subparsers = self._parser.add_subparsers(dest='command')
        self._build_main_command()
        self._build_pipeline_command()
        self._build_dashboard_command()

    @property
    def args(self) -> dict[str, Any]:
        if self._args is None:
            self._args = vars(self._parser.parse_args())
        return self._args

    def extract_data_lake(self) -> tuple[DataLakeImpl, Path]:
        match self.args:
            case {'redis': Path(), 's3_bucket': None}:
                return DataLakeImpl.REDIS, self.args['redis']
            case {'s3_bucket': Path(), 'redis': None}:
                return DataLakeImpl.S3BUCKET, self.args['s3_bucket']
            case _:
                raise ValueError(
                    'Parsed arguments resulted in unsupported or invalid data'
                    ' lake configuration'
                )

    def extract_data_warehouse(self) -> tuple[EtlLoaderImpl, Path]:
        match self.args:
            case {'mongodb': Path(), 'sql': None}:
                return EtlLoaderImpl.MONGODB, self.args['mongodb']
            case {'sql': Path(), 'mongodb': None}:
                return EtlLoaderImpl.SQL, self.args['sql']
            case _:
                raise ValueError(
                    'Parsed arguments resulted in unsupported or invalid ETL '
                    'loader configuration'
                )

    def extract_data_provider(self) -> tuple[DashboardProviderImpl, Path]:
        match self.args:
            case {'mongodb': Path()}:
                return DashboardProviderImpl.MONGODB, self.args['mongodb']
            case _:
                raise ValueError(
                    'Parsed arguments resulted in unsupported or invalid '
                    'dashboard data provider configuration'
                )

    def _build_main_command(self):
        self._parser.add_argument(
            '-l', '--log-path', default=Path('var/it_jobs_meta.log'), type=Path
        )

    def _build_pipeline_command(self):
        parser_pipeline = self._subparsers.add_parser('pipeline')

        parser_pipeline.add_argument(
            '-c', '--schedule', metavar='CRON_EXPRESSION', action='store'
        )
        data_lake_arg_grp = parser_pipeline.add_mutually_exclusive_group(
            required=True
        )
        data_lake_arg_grp.add_argument(
            '-r', '--redis', metavar='CONFIG_PATH', action='store', type=Path
        )
        data_lake_arg_grp.add_argument(
            '-b',
            '--s3-bucket',
            metavar='CONFIG_PATH',
            action='store',
            type=Path,
        )

        etl_loader_arg_grp = parser_pipeline.add_mutually_exclusive_group(
            required=True
        )
        etl_loader_arg_grp.add_argument(
            '-m', '--mongodb', metavar='CONFIG_PATH', action='store', type=Path
        )
        etl_loader_arg_grp.add_argument(
            '-s', '--sql', metavar='CONFIG_PATH', action='store', type=Path
        )

    def _build_dashboard_command(self):
        parser_dashboard = self._subparsers.add_parser('dashboard')

        parser_dashboard.add_argument(
            '-w', '--with-wsgi', action='store_true', default=False
        )

        data_provider_arg_grp = parser_dashboard.add_mutually_exclusive_group(
            required=True
        )
        data_provider_arg_grp.add_argument(
            '-m', '--mongodb', metavar='CONFIG_PATH', action='store', type=Path
        )
