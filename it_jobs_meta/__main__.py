from pathlib import Path

import typer

from it_jobs_meta.common.utils import setup_logging
from it_jobs_meta.dashboard.dashboard import run_dashboard
from it_jobs_meta.data_pipeline.data_pipeline import run_data_pipeline


it_jobs_app = typer.Typer()


@it_jobs_app.command()
def data_pipeline(
    data_lake_config_path: Path, data_warehouse_config_path: Path
):
    setup_logging()
    run_data_pipeline(data_lake_config_path, data_warehouse_config_path)


@it_jobs_app.command()
def dashboard(data_warehouse_config_path: Path):
    setup_logging()
    run_dashboard(data_warehouse_config_path)


def main():
    it_jobs_app()


if __name__ == '__main__':
    main()
