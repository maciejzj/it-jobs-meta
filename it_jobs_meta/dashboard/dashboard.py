import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum, auto
from functools import partial
from pathlib import Path
from typing import Optional, Type

import dash
import dash_bootstrap_components as dbc
import pandas as pd
import sqlalchemy as db
from dash import dcc, html
from dash.development import base_component as DashComponent
from flask_caching import Cache as AppCache
from plotly import graph_objects as go

from it_jobs_meta.common.utils import setup_logging
from it_jobs_meta.data_pipeline.data_warehouse import (
    load_warehouse_db_config,
    make_db_uri_from_config,
)

from .dashboard_components import (
    CategoriesPieChart,
    CategoriesTechnologiesSankeyChart,
    ContractTypeViolinChart,
    RemotePieChart,
    SalariesMap,
    SalariesMapJunior,
    SalariesMapMid,
    SalariesMapSenior,
    SenioritiesHistogram,
    SeniorityPieChart,
    TechnologiesPieChart,
    TechnologiesViolinChart,
)


class TableNames(Enum):
    METADATA = 'metadata'
    POSTINGS = 'postings'
    LOCATIONS = 'locations'
    SALARIES = 'salaries'
    SENIORITIES = 'seniorities'


class Graphs(Enum):
    REMOTE_PIE_CHART = auto()
    TECHNOLOGIES_PIE_CHART = auto()
    CATEGORIES_PIE_CHART = auto()
    SENIORITY_PIE_CHART = auto()
    CAT_TECH_SANKEY_CHART = auto()
    SALARIES_MAP = auto()
    SENIORITIES_HISTOGRAM = auto()
    TECHNOLOGIES_VIOLIN_PLOT = auto()
    CONTRACT_TYPE_VIOLIN_PLOT = auto()
    SALARIES_MAP_JUNIOR = auto()
    SALARIES_MAP_MID = auto()
    SALARIES_MAP_SENIOR = auto()


@dataclass
class DynamicContent:
    obtained_datetime: datetime
    graphs: dict[Graphs, go.Figure]


@dataclass
class DashboardTextualComponents:
    JUMBOTRON_TEXT_MD = '''
    Discover current trends in the IT job market in Poland through
    **interactive** graphs that are created based on real data gathered from
    **No Fluff Jobs**—one of the leading job walls in the country.
    '''

    ABOUT_TEXT_MD = '''
    This website serves as a data analysis service based on postings from
    [No Fluff Jobs](https://nofluffjobs.com), which is one of the most popular
    job walls for IT specialists in Poland. The data is gathered once a week and
    presented to you in form of beautiful plots, graphs, and maps.  The
    meta-analysis of IT markets provides insights into leading technologies,
    salaries, job experience, and work locations distributions. The knowledge
    distilled from the analysis can help you with finding a job, evaluating
    salary for a vacancy, or planning your career in the IT sector. Have fun
    exploring the data!
    '''


def gather_data(
    data_warehouse_config_path: Path,
) -> dict[TableNames, pd.DataFrame]:
    db_config = load_warehouse_db_config(data_warehouse_config_path)
    db_con = db.create_engine(make_db_uri_from_config(db_config))

    data_tables = {}
    for table_name in TableNames:
        data_tables[table_name] = pd.read_sql_table(table_name.value, db_con)
    return data_tables


def make_graphs(
    data: dict[TableNames, pd.DataFrame]
) -> dict[Graphs, go.Figure]:
    graphs = {
        Graphs.CATEGORIES_PIE_CHART: dcc.Graph(
            figure=CategoriesPieChart.make_fig(data[TableNames.POSTINGS])
        ),
        Graphs.TECHNOLOGIES_PIE_CHART: dcc.Graph(
            figure=TechnologiesPieChart.make_fig(data[TableNames.POSTINGS])
        ),
        Graphs.CAT_TECH_SANKEY_CHART: dcc.Graph(
            figure=CategoriesTechnologiesSankeyChart.make_fig(
                data[TableNames.POSTINGS]
            )
        ),
        Graphs.SENIORITY_PIE_CHART: dcc.Graph(
            figure=SeniorityPieChart.make_fig(data[TableNames.SENIORITIES])
        ),
        Graphs.SENIORITIES_HISTOGRAM: dcc.Graph(
            figure=SenioritiesHistogram.make_fig(
                data[TableNames.SENIORITIES], data[TableNames.SALARIES]
            )
        ),
        Graphs.REMOTE_PIE_CHART: dcc.Graph(
            figure=RemotePieChart.make_fig(data[TableNames.POSTINGS])
        ),
        Graphs.SALARIES_MAP_JUNIOR: dcc.Graph(
            figure=SalariesMapJunior.make_fig(
                data[TableNames.LOCATIONS],
                data[TableNames.SALARIES],
                data[TableNames.SENIORITIES],
            )
        ),
        Graphs.SALARIES_MAP_MID: dcc.Graph(
            figure=SalariesMapMid.make_fig(
                data[TableNames.LOCATIONS],
                data[TableNames.SALARIES],
                data[TableNames.SENIORITIES],
            )
        ),
        Graphs.SALARIES_MAP_SENIOR: dcc.Graph(
            figure=SalariesMapSenior.make_fig(
                data[TableNames.LOCATIONS],
                data[TableNames.SALARIES],
                data[TableNames.SENIORITIES],
            )
        ),
        Graphs.SALARIES_MAP: dcc.Graph(
            figure=SalariesMap.make_fig(
                data[TableNames.LOCATIONS], data[TableNames.SALARIES]
            )
        ),
        Graphs.TECHNOLOGIES_VIOLIN_PLOT: dcc.Graph(
            figure=TechnologiesViolinChart.make_fig(
                data[TableNames.POSTINGS],
                data[TableNames.SALARIES],
                data[TableNames.SENIORITIES],
            )
        ),
        Graphs.CONTRACT_TYPE_VIOLIN_PLOT: dcc.Graph(
            figure=ContractTypeViolinChart.make_fig(
                data[TableNames.POSTINGS], data[TableNames.SALARIES]
            )
        ),
    }
    return graphs


def make_dynamic_content(
    data: dict[TableNames, pd.DataFrame]
) -> DynamicContent:
    obtained_datetime = pd.to_datetime(
        data[TableNames.METADATA]['obtained_datetime'][0]
    )
    graphs = make_graphs(data)
    return DynamicContent(obtained_datetime=obtained_datetime, graphs=graphs)


def make_navbar() -> DashComponent:
    navbar = dbc.NavbarSimple(
        dbc.NavLink(
            dbc.Button(
                [html.I(className='fab fa-github'), ' GitHub'], color='dark'
            ),
            href='https://github.com/maciejzj/no-fluff-meta',
            active=True,
            className='mr-0',
        ),
        brand='IT Jobs Meta',
        className='bg-white',
    )
    return navbar


def make_jumbotron() -> DashComponent:
    jumbotron = html.Section(
        dbc.Row(
            [
                dbc.Col(
                    html.Div(
                        children=[
                            html.H1(
                                'Weekly analysis of IT job offers in Poland'
                            ),
                            dcc.Markdown(
                                DashboardTextualComponents.JUMBOTRON_TEXT_MD
                            ),
                            dbc.Button(
                                'To the data',
                                href='#data-section',
                                outline=True,
                                external_link=True,
                                active=True,
                                color='light',
                            ),
                        ],
                        className='p-5 text-white bg-dark rounded shadow-lg',
                    ),
                    lg=6,
                    className='mt-5',
                )
            ]
        )
    )
    return jumbotron


def make_about() -> DashComponent:
    about = html.Section(
        [
            html.H2('About'),
            dcc.Markdown(DashboardTextualComponents.ABOUT_TEXT_MD),
        ],
        className='text-center mt-5',
    )
    return about


def make_grahps_layout_header(obtained_datetime: datetime) -> DashComponent:
    datetime_str = obtained_datetime.strftime('%-d %B %Y')
    div = html.Div(
        [
            html.H2('Data', id='data-container'),
            html.B(f'Last obtained on {datetime_str}'),
            html.P(
                '''Although this website works fine on mobile devices, it
                is most convenient to explore more complex graphs on larger
                screens.'''
            ),
        ],
        className='text-center mt-5',
    )
    return div


def make_categories_and_seniorities_graphs_layout(
    graphs: dict[Graphs, go.Figure]
) -> DashComponent:
    div = html.Div(
        [
            html.H3('Categories and seniorities', className='mt-4'),
            dbc.Row(
                [
                    dbc.Col(
                        dbc.Card(
                            graphs[Graphs.CATEGORIES_PIE_CHART],
                            className='mt-4 p-1 border-0 rounded shadow',
                        ),
                        md=6,
                    ),
                    dbc.Col(
                        dbc.Card(
                            graphs[Graphs.TECHNOLOGIES_PIE_CHART],
                            className='mt-4 p-1 border-0 rounded shadow',
                        ),
                        md=6,
                    ),
                ],
                align='center',
            ),
            dbc.Row(
                [
                    dbc.Col(
                        dbc.Card(
                            graphs[Graphs.CAT_TECH_SANKEY_CHART],
                            className='mt-4 p-1 border-0 rounded shadow',
                        ),
                        md=7,
                    ),
                    dbc.Col(
                        dbc.Card(
                            graphs[Graphs.SENIORITY_PIE_CHART],
                            className='mt-4 p-1 border-0 rounded shadow',
                        ),
                        md=5,
                    ),
                ],
                align='center',
            ),
            dbc.Card(
                graphs[Graphs.SENIORITIES_HISTOGRAM],
                className='mt-4 p-1 border-0 rounded shadow',
            ),
        ]
    )
    return div


def make_locations_and_remote_graphs_layout(
    graphs: dict[Graphs, go.Figure]
) -> DashComponent:
    div = html.Div(
        [
            html.H3('Locations and remote', className='mt-4'),
            dbc.Row(
                [
                    dbc.Col(
                        dbc.Card(
                            graphs[Graphs.REMOTE_PIE_CHART],
                            className='mt-4 p-1 border-0 rounded shadow',
                        ),
                        lg=6,
                    ),
                    dbc.Col(
                        dbc.Card(
                            graphs[Graphs.SALARIES_MAP],
                            className='mt-4 p-1 border-0 rounded shadow',
                        ),
                        lg=6,
                    ),
                ],
                align='center',
            ),
            dbc.Row(
                [
                    dbc.Col(
                        dbc.Card(
                            graphs[Graphs.SALARIES_MAP_JUNIOR],
                            className='mt-4 p-1 border-0 rounded shadow',
                        ),
                        lg=4,
                    ),
                    dbc.Col(
                        dbc.Card(
                            graphs[Graphs.SALARIES_MAP_MID],
                            className='mt-4 p-1 border-0 rounded shadow',
                        ),
                        lg=4,
                    ),
                    dbc.Col(
                        dbc.Card(
                            graphs[Graphs.SALARIES_MAP_SENIOR],
                            className='mt-4 p-1 border-0 rounded shadow',
                        ),
                        lg=4,
                    ),
                ],
                align='center',
            ),
        ]
    )
    return div


def make_salaries_breakdown_graphs_layout(
    graphs: dict[Graphs, go.Figure]
) -> DashComponent:
    div = html.Div(
        [
            html.H3('Salaries breakdown', className='mt-4'),
            dbc.Card(
                graphs[Graphs.TECHNOLOGIES_VIOLIN_PLOT],
                className='mt-4 p-1 border-0 rounded shadow',
            ),
            dbc.Card(
                graphs[Graphs.CONTRACT_TYPE_VIOLIN_PLOT],
                className='mt-4 p-1 border-0 rounded shadow',
            ),
        ]
    )
    return div


def make_graphs_layout(
    obtained_datetime: datetime, graphs: dict[Graphs, go.Figure]
) -> DashComponent:
    data_section = html.Section(
        [
            make_grahps_layout_header(obtained_datetime),
            make_categories_and_seniorities_graphs_layout(graphs),
            make_locations_and_remote_graphs_layout(graphs),
            make_salaries_breakdown_graphs_layout(graphs),
        ],
        id='data-section',
    )
    return data_section


def make_footer() -> DashComponent:
    footer = html.Footer(
        [
            html.Div(html.Strong('Maciej Ziaja')),
            html.Div('maciejzj@icloud.com'),
            dbc.Button(
                [html.I(className='fab fa-github'), ' GitHub'],
                color='dark',
                href='https://github.com/maciejzj/no-fluff-meta',
                className='mt-2',
            ),
        ],
        className='m-3 p-3 text-center',
    )
    return footer


def make_layout(dynamic_content: DynamicContent) -> DashComponent:
    layout = html.Div(
        children=[
            make_navbar(),
            dbc.Container(
                [
                    make_jumbotron(),
                    make_about(),
                    make_graphs_layout(
                        dynamic_content.obtained_datetime,
                        dynamic_content.graphs,
                    ),
                ],
                className='pb-3',
            ),
            make_footer(),
        ],
        className='w-100',
    )
    return layout


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
    cache_timeout_seconds: int = int(timedelta(seconds=30).total_seconds())
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
    logging.info('Attempting to retrieve data from warehouse on startup')
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
