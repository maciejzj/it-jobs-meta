import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from functools import partial
from pathlib import Path
from abc import ABC, abstractmethod
from typing import Optional, Type

import dash
import dash_bootstrap_components as dbc
import pandas as pd
import pymongo
from dash.development import base_component as DashComponent
from flask_caching import Cache as AppCache
from plotly import graph_objects as go

from it_jobs_meta.common.utils import setup_logging
from it_jobs_meta.data_pipeline.data_warehouse import (
    load_warehouse_db_config,
)
from it_jobs_meta.dashboard.layout import DynamicContent, make_layout


from .dashboard_components import (
    GraphRegistry,
    Graphs,
)


@dataclass
class GatheredData:
    metadata: pd.DataFrame
    postings: pd.DataFrame


class DashboardDataProvider(ABC):
    @abstractmethod
    def gather_data(self) -> GatheredData:
        pass


class MongoDbDashboardDataProvider(DashboardDataProvider):
    def __init__(
        self, user_name: str, password: str, host: str, db_name: str, port=27017
    ):
        self._db_client = pymongo.MongoClient(
            f'mongodb://{user_name}:{password}@{host}:{port}'
        )
        self._db = self._db_client[db_name]

    def gather_data(self) -> GatheredData:
        metadata_df = pd.json_normalize(self._db['metadata'].find())
        postings_df = pd.json_normalize(self._db['postings'].find())
        return GatheredData(metadata=metadata_df, postings=postings_df)


def gather_data(
    data_warehouse_config_path: Path,
) -> GatheredData:
    db_conf = load_warehouse_db_config(data_warehouse_config_path)
    data_provider = MongoDbDashboardDataProvider(
        user_name=db_conf.user_name,
        password=db_conf.password,
        host=db_conf.host_address,
        db_name=db_conf.db_name,
    )
    return data_provider.gather_data()


def make_graphs(data: pd.DataFrame) -> dict[Graphs, go.Figure]:
    return GraphRegistry.make(data)


def make_dynamic_content(data: GatheredData) -> DynamicContent:
    obtained_datetime = pd.to_datetime(data.metadata['obtained_datetime'][0])
    graphs = make_graphs(data.postings)
    return DynamicContent(obtained_datetime=obtained_datetime, graphs=graphs)


def make_dash_app() -> dash.Dash:
    app = dash.Dash(
        'it-jobs-meta-dashboard',
        assets_folder='it_jobs_meta/dashboard/assets',
        external_stylesheets=[dbc.themes.BOOTSTRAP, dbc.icons.FONT_AWESOME],
        title='IT Jobs Meta',
        meta_tags=[
            {
                'description': 'Weekly analysis of IT job offers in Poland',
                'keywords': 'Programming, Software, IT, Jobs',
                'name': 'viewport',
                'content': 'width=device-width, initial-scale=1',
            },
        ],
    )
    return app


def make_app_cache(app: dash.Dash) -> AppCache:
    cache = AppCache(
        app.server,
        config={
            'CACHE_TYPE': 'filesystem',
            'CACHE_DIR': '.cache',
        },
    )
    return cache


class App:
    cache_timeout_seconds: int = int(timedelta(seconds=60).total_seconds())
    _app: Optional[dash.Dash] = None
    _cache: Optional[AppCache] = None

    @classmethod
    @property
    def app(cls) -> dash.Dash:
        if cls._app is None:
            cls._app = make_dash_app()
        return cls._app

    @classmethod
    @property
    def cache(cls) -> AppCache:
        if cls._cache is None:
            cls._cache = make_app_cache(cls.app)
        return cls._cache


@App.cache.memoize(timeout=App.cache_timeout_seconds)
def render_dashboard(data_warehouse_config_path: Path) -> DashComponent:
    logging.info('Rendering dashboard')
    logging.info('Attempting to retrieve data')
    data = gather_data(data_warehouse_config_path)
    logging.info('Data retrieval succeeded')

    logging.info('Making layout')
    dynamic_content = make_dynamic_content(data)
    layout = make_layout(dynamic_content)
    logging.info('Making layout succeeded')
    logging.info('Rendering dashboard succeeded')
    return layout


def make_dashboard_app(data_warehouse_config_path: Path) -> Type[App]:
    App.app.layout = partial(
        render_dashboard,
        data_warehouse_config_path=data_warehouse_config_path,
    )
    return App


def run_dashboard(data_warehouse_config_path: Path):
    try:
        App = make_dashboard_app(data_warehouse_config_path)
        App.app.run_server(debug=True, host='0.0.0.0')
    except Exception as e:
        logging.exception(e)
        raise


def main():
    setup_logging()
    data_warehouse_config_path = Path('config/warehouse_db_config.yaml')
    run_dashboard(data_warehouse_config_path)


if __name__ == '__main__':
    main()
