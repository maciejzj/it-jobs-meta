from multiprocessing.sharedctypes import Value
from pathlib import Path
import argparse
from it_jobs_meta.data_pipeline import data_warehouse
from enum import Enum, auto
from typing import Any

from it_jobs_meta.common.utils import setup_logging
from it_jobs_meta.dashboard.dashboard import run_dashboard
from it_jobs_meta.data_pipeline.data_lake import RedisDataLake, S3DataLake
from it_jobs_meta.data_pipeline.data_pipeline import run_data_pipeline, run_pipeline_in_schedule, run_data_pipeline_f, load_data_lake_db_config
from it_jobs_meta.data_pipeline.data_warehouse import PandasEtlNoSqlLoadingEngine, PandasEtlSqlLoadingEngine, PandasEtlExtractionFromJsonStr, PandasEtlTransformationEngine, EtlPipeline, load_warehouse_db_config


class DataLakes(Enum):
   redis = auto()
   s3bucket = auto()

class DataLakeFactory:
    def __init__(self, kind: DataLakes):
        self.kind = kind

    def make(self, config_path: Path):
        match self.kind:
            case DataLakes.redis:
                config = load_data_lake_db_config(config_path)
                return RedisDataLake(config)
            case DataLakes.s3bucket:
                config = load_data_lake_db_config(config_path)
                return S3DataLake(config)
            case _:
                return ValueError('Selected data lake implementation is not supported or invalid')


class DataWarehouses(Enum):
   MONGODB = auto()
   SQL = auto()


class EtlPipelineFactory:
    def __init__(self, kind: DataLakes):
        self.kind = kind

    def make(self, config_path: Path):
        match self.kind:
            case DataWarehouses.MONGODB:
                extracor = PandasEtlExtractionFromJsonStr()
                transformer = PandasEtlTransformationEngine()
                db_conf = load_warehouse_db_config(config_path)
                loader = PandasEtlNoSqlLoadingEngine(
                    user_name=db_conf.user_name,
                    password=db_conf.password,
                    host=db_conf.host_address,
                    db_name=db_conf.db_name,
                )
                return EtlPipeline(extracor, transformer, loader)
            case DataLakes.s3bucket:
                extracor = PandasEtlExtractionFromJsonStr()
                transformer = PandasEtlTransformationEngine()
                db_conf = load_warehouse_db_config(config_path)
                loader = PandasEtlSqlLoadingEngine(
                    user_name=db_conf.user_name,
                    password=db_conf.password,
                    host=db_conf.host_address,
                    db_name=db_conf.db_name,
                )
                return EtlPipeline(extracor, transformer, loader)
        

def extract_data_lake(args: dict[str, Any]) -> tuple[DataLakes, Path]:
    match args:
        case {'redis': Path(), 's3_bucket': None}:
            return DataLakes.redis, args['redis']
        case {'redis': None, 's3_bucket': Path()}:
            return DataLakes.s3bucket, args['s3_bucket']
        case _:
            raise ValueError('Parsed arguments resulted in unsupported or invalid data lake configuration')


def extract_data_warehouse(args: dict[str, Any]) -> tuple[DataWarehouses, Path]:
    match args:
        case {'mongodb': Path(), 'sql': None}:
            return DataWarehouses.MONGODB, args['mongodb']
        case {'mongodb': None, 'sql': Path()}:
            return DataWarehouses.SQL, args['sql']
        case _:
            raise ValueError('Parsed arguments resulted in unsupported or invalid data warehouse configuration')


def main():
    setup_logging()
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest='command')
    parser_data_pipeline = subparsers.add_parser('pipeline')
    parser_dashboard = subparsers.add_parser('dashboard')

    parser_data_pipeline.add_argument('-c', '--schedule', metavar='CRON_EXPRESSION', action='store')
    data_lake_arg_grp = parser_data_pipeline.add_mutually_exclusive_group(required=True)
    data_lake_arg_grp.add_argument('-r', '--redis', metavar='CONFIG_PATH', action='store', type=Path)
    data_lake_arg_grp.add_argument('-b', '--s3-bucket', metavar='CONFIG_PATH', action='store', type=Path)


    data_warehouse_arg_grp = parser_data_pipeline.add_mutually_exclusive_group(required=True)
    data_warehouse_arg_grp.add_argument('-m', '--mongodb', metavar='CONFIG_PATH', action='store', type=Path)
    data_warehouse_arg_grp.add_argument('-s', '--sql', metavar='CONFIG_PATH', action='store', type=Path)

    data_warehouse_arg_grp = parser_dashboard.add_mutually_exclusive_group(required=True)
    data_warehouse_arg_grp.add_argument('-m', '--mongodb', metavar='CONFIG_PATH', action='store', type=Path)

    args = parser.parse_args()
    args = vars(args)

    match args['command']:
        case 'pipeline':
            data_lake_type, data_lake_cfg_path = extract_data_lake(args)
            data_warehouse_type, data_warehouse_cfg_path = extract_data_warehouse(args)
            data_lake_factory = DataLakeFactory(data_lake_type)
            data_warehouse_factory = EtlPipelineFactory(data_warehouse_type)

            if args['schedule'] is not None:
                run_pipeline_in_schedule(args['schedule'], data_lake_factory, data_warehouse_factory, data_lake_cfg_path, data_warehouse_cfg_path)
            else:
                run_data_pipeline_f(data_lake_factory, data_warehouse_factory, data_lake_cfg_path, data_warehouse_cfg_path)
        case 'dashboard':
            run_dashboard(args['mongodb'])




    # parser_dashboard = subparsers.add_parser('dashboard')


if __name__ == '__main__':
    main()
