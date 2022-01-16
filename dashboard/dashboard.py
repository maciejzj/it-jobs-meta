from pathlib import Path

import dash
import dash_bootstrap_components as dbc
import pandas as pd
import sqlalchemy as db
from dash import dcc
from dash import html

from data_processing.data_warehouse import (
    make_db_uri_from_config,
    load_warehouse_db_config)

from .dashboard_components import (
    RemotePieChart,
    SalariesMap,
    SalariesSenioritiesMapChart,
    SenioritiesHistogram,
    TechnologiesPieChart,
    CategoriesPieChart,
    SeniorityPieChart,
    CategoriesTechnologiesSankeyChart,
    ContractTypeViolinChart,
    TechnologiesViolinChart,
    SalariesMapJunior,
    SalariesMapMid,
    SalariesMapSenior
)


def gather_data():
    db_config = load_warehouse_db_config(
        Path('data_processing/config/warehouse_db_config.yaml'))
    db_con = db.create_engine(make_db_uri_from_config(db_config))

    ret = {}
    TABLE_NAMES = ('postings', 'locations', 'salaries', 'seniorities')
    for table_name in TABLE_NAMES:
        ret[table_name] = pd.read_sql_table(table_name, db_con)
    return ret


def make_graphs(data):
    graphs = {
        'remote_pie_chart':
            dcc.Graph(figure=RemotePieChart.make_fig(data['postings'])),
        'technologies_pie_chart':
            dcc.Graph(figure=TechnologiesPieChart.make_fig(data['postings'])),
        'categories_pie_chart':
            dcc.Graph(figure=CategoriesPieChart.make_fig(data['postings'])),
        'seniority_pie_chart':
            dcc.Graph(figure=SeniorityPieChart.make_fig(data['seniorities'])),
        'cat_tech_sankey_chart':
            dcc.Graph(figure=CategoriesTechnologiesSankeyChart.make_fig(
                data['postings'])),
        'salaries_map': dcc.Graph(figure=SalariesMap.make_fig(
            data['locations'], data['salaries'])),
        'seniorities_histogram':
            dcc.Graph(figure=SenioritiesHistogram.make_fig(
                data['seniorities'], data['salaries'])),
        'technologies_violin_plot':
            dcc.Graph(figure=TechnologiesViolinChart.make_fig(
                data['postings'], data['salaries'], data['seniorities'])),
        'contract_type_violin_plot':
            dcc.Graph(figure=ContractTypeViolinChart.make_fig(
                data['postings'], data['salaries'])),
        'salaries_map_junior':
            dcc.Graph(figure=SalariesMapJunior.make_fig(
                data['locations'], data['salaries'], data['seniorities'])),
        'salaries_map_mid':
            dcc.Graph(figure=SalariesMapMid.make_fig(
                data['locations'], data['salaries'], data['seniorities'])),
        'salaries_map_senior':
            dcc.Graph(figure=SalariesMapSenior.make_fig(
                data['locations'], data['salaries'], data['seniorities'])),
    }
    return graphs


def make_layout():
    data = gather_data()
    graphs = make_graphs(data)

    layout = html.Div(children=[
        dbc.NavbarSimple(
            dbc.NavLink(
                dbc.Button([html.I(className="fab fa-github"), ' GitHub'],
                           color='dark'), active=True, className='mr-0'),
            brand='It Jobs Meta', className='bg-white'),

        dbc.Container([
            dbc.Row([
                dbc.Col(
                    html.Div(children=[
                        html.H1('Daily analysis of IT job offers in Poland'),
                        html.P('''
                            Lorem Ipsum is simply dummy text of the printing
                            and typesetting industry. Lorem Ipsum has been the
                            industry's standard dummy text ever since the
                            1500s, when an unknown printer took a galley of
                            type and scrambled it to make a type specimen book.
                        '''),
                        dbc.Button('To the data', outline=True,
                                   active=True, color='light')
                    ], className='p-5 text-white bg-dark rounded shadow-lg',
                    ),
                    md=6,
                    className='mt-5',
                   )
            ]),

            dbc.Container([
                html.H2('About'),
                html.P('''
                Lorem Ipsum is simply dummy text of the printing and
                typesetting industry. Lorem Ipsum has been the industry's
                standard dummy text ever since the 1500s, when an unknown
                printer took a galley of type and scrambled it to make a type
                specimen book. It has survived not only five centuries, but
                also the leap into electronic typesetting, remaining
                essentially unchanged. It was popularised in the 1960s with the
                release of Letraset sheets containing Lorem Ipsum passages, and
                more recently with desktop publishing software like Aldus
                PageMaker including
                versions of Lorem Ipsum.
            '''),
            ], className='text-center mt-5'),

            dbc.Container([
                html.H2('Data'),
                html.B('Last obtained on 21.12.21'),
            ], className='text-center mt-5'),

            html.H3('Categories and seniorities', className='mt-4'),
            dbc.Row([
                dbc.Col(dbc.Card(graphs['categories_pie_chart'],
                        className='mt-4 p-1 border-0 rounded shadow'), md=6),
                dbc.Col(dbc.Card(graphs['technologies_pie_chart'],
                        className='mt-4 p-1 border-0 rounded shadow'), md=6),
            ], align='center'),
            dbc.Row([
                dbc.Col(dbc.Card(graphs['cat_tech_sankey_chart'],
                        className='mt-4 p-1 border-0 rounded shadow'), md=7),
                dbc.Col(dbc.Card(graphs['seniority_pie_chart'],
                        className='mt-4 p-1 border-0 rounded shadow'), md=5),
            ], align='center'),
            dbc.Card(graphs['seniorities_histogram'],
                     className='mt-4 p-1 border-0 rounded shadow'),

            html.H3('Locations and remote', className='mt-4'),
            dbc.Row([
                dbc.Col(dbc.Card(graphs['remote_pie_chart'],
                    className='mt-4 p-1 border-0 rounded shadow'), md=6),
                dbc.Col(dbc.Card(graphs['salaries_map'],
                    className='mt-4 p-1 border-0 rounded shadow'), md=6),
            ], align='center'),
            dbc.Row([
                dbc.Col(dbc.Card(graphs['salaries_map_junior'],
                        className='mt-4 p-1 border-0 rounded shadow'), md=4),
                dbc.Col(dbc.Card(graphs['salaries_map_mid'],
                        className='mt-4 p-1 border-0 rounded shadow'), md=4),
                dbc.Col(dbc.Card(graphs['salaries_map_senior'],
                        className='mt-4 p-1 border-0 rounded shadow'), md=4),
            ], align='center'),

            html.H3('Salaries breakdown', className='mt-4'),
            dbc.Card(graphs['technologies_violin_plot'],
                     className='mt-4 p-1 border-0 rounded shadow'),
            dbc.Card(graphs['contract_type_violin_plot'],
                     className='mt-4 p-1 border-0 rounded shadow'),
        ]),
    ], className='w-100')
    return layout


def make_dash_app():
    app = dash.Dash(
        'it-jobs-meta-dashboard',
        external_stylesheets=[
            dbc.themes.BOOTSTRAP, dbc.icons.FONT_AWESOME],
        meta_tags=[
            {"name": "viewport",
             "content": "width=device-width, initial-scale=1"},
        ])
    return app


if __name__ == '__main__':
    app = make_dash_app()
    app.layout = make_layout()
    app.run_server(debug=True, host='0.0.0.0')
