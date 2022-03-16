import logging
from datetime import timedelta
from pathlib import Path

import dash
import dash_bootstrap_components as dbc
import pandas as pd
from dash.development import base_component as DashComponent
from flask_caching import Cache as AppCache
from waitress import serve as wsgi_serve

from it_jobs_meta.common.utils import setup_logging
from it_jobs_meta.dashboard.dashboard_components import GraphRegistry
from it_jobs_meta.dashboard.data_provision import (
    DashboardDataProviderFactory,
    DashboardProviderImpl,
    GatheredData,
)
from it_jobs_meta.dashboard.layout import DynamicContent, make_layout


class DashboardApp:
    def __init__(
        self,
        data_provider_factory: DashboardDataProviderFactory,
        cache_timeout=timedelta(hours=6),
    ):
        self._app: dash.Dash | None = None
        self._cache: AppCache | None = None
        self._data_provider_factory = data_provider_factory
        self._cache_timeout = cache_timeout

    @property
    def app(self) -> dash.Dash:
        if self._app is None:
            self._app = dash.Dash(
                'it-jobs-meta-dashboard',
                assets_folder='it_jobs_meta/dashboard/assets',
                external_stylesheets=[
                    dbc.themes.BOOTSTRAP,
                    dbc.icons.FONT_AWESOME,
                ],
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
        return self._app

    @property
    def cache(self) -> AppCache:
        if self._cache is None:
            self._cache = AppCache(
                self.app.server,
                config={
                    'CACHE_TYPE': 'SimpleCache',
                },
            )
        return self._cache

    def render_layout(self) -> DashComponent:
        logging.info('Rendering dashboard')
        logging.info('Attempting to retrieve data')
        data = self._data_provider_factory.make().gather_data()
        logging.info('Data retrieval succeeded')

        logging.info('Making layout')
        dynamic_content = self.make_dynamic_content(data)
        layout = make_layout(dynamic_content)
        logging.info('Making layout succeeded')
        logging.info('Rendering dashboard succeeded')
        return layout

    def run(self, with_wsgi=False):
        try:
            render_layout_memoized = self.cache.memoize(
                timeout=int(self._cache_timeout.total_seconds())
            )(self.render_layout)
            self.app.layout = render_layout_memoized

            if with_wsgi:
                wsgi_serve(self.app.server, host='0.0.0.0', port='80')
            else:
                self.app.run_server(debug=True, host='0.0.0.0', port='80')

        except Exception as e:
            logging.exception(e)
            raise

    @staticmethod
    def make_dynamic_content(data: GatheredData) -> DynamicContent:
        obtained_datetime = pd.to_datetime(
            data.metadata['obtained_datetime'][0]
        )
        graphs = GraphRegistry.make(data.postings)
        return DynamicContent(
            obtained_datetime=obtained_datetime, graphs=graphs
        )


def main():
    setup_logging()
    data_warehouse_config_path = Path('config/db_config_mongo.yaml')
    data_warehouse_factory = DashboardDataProviderFactory(
        DashboardProviderImpl.MONGODB, data_warehouse_config_path
    )
    app = DashboardApp(
        data_warehouse_factory, cache_timeout=timedelta(seconds=30)
    )
    app.run()


if __name__ == '__main__':
    main()
