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
    SalariesMapChart,
    SalariesSenioritiesMapChart,
    SenioritiesHistogram,
    TechnologiesPieChart,
    CategoriesPieChart,
    SeniorityPieChart,
    CategoriesTechnologiesSankeyChart,
    ContractTypeViolinChart,
    TechnologiesViolinChart,
)


def gather_data():
    db_config = load_warehouse_db_config(
        Path('data_processing/warehouse_db_config.yaml'))
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
        'salaries_map': dcc.Graph(figure=SalariesMapChart.make_fig(
            data['locations'], data['salaries'])),
        'salaries_seniorities_map':
            dcc.Graph(figure=SalariesSenioritiesMapChart.make_fig(
                data['locations'], data['salaries'], data['seniorities'])),
        'seniorities_histogram':
            dcc.Graph(figure=SenioritiesHistogram.make_fig(
                data['seniorities'], data['salaries'])),
        'technologies_violin_plot':
            dcc.Graph(figure=TechnologiesViolinChart.make_fig(
                data['postings'], data['salaries'], data['seniorities'])),
        'contract_type_violin_plot':
            dcc.Graph(figure=ContractTypeViolinChart.make_fig(
                data['postings'], data['salaries']))
    }
    return graphs


def make_layout():
    data = gather_data()
    graphs = make_graphs(data)

    layout = html.Div(children=[
        dbc.NavbarSimple(dbc.NavLink(dbc.Button([html.I(className="fab fa-github"), ' GitHub'],
                         outline=True, color='light'), active=True), brand='It Jobs Meta', color='dark', dark=True),
        dbc.Container([

            html.Div(
                dbc.Container([
                    html.H1('Daily data about IT jobs market in Poland'),
                    html.Hr(className='my-2'),
                    html.P([
                        'Meta-analysis of job postings from ',
                        dbc.Badge('No Fluff Jobs', href='https://nofluffjobs.com',
                                  color='primary', className='text-decoration-none')],
                           className='lead'),
                    html.P(
                        'A place for software developers to get daily '
                        'statistical meta-analysis of online-posted job '
                        'offers.'
                    ),
                    dbc.Accordion([
                        dbc.AccordionItem([
                            html.P("What is this site?"),
                            dbc.Button("Click here"),
                        ], title="What is this site?"),
                        dbc.AccordionItem([
                            html.P("How can I use this?"),
                            dbc.Button("Don't click me!", color="danger"),
                        ], title="How can I use this site?"),
                        dbc.AccordionItem(
                            "",
                            title="How does this work?"),
                    ]),
                    dbc.Alert(['Data last obtained on: ', html.Strong('15th Decemember 2021')],
                              color='primary', className='mt-3'),
                ], fluid=True, className=''),
                className='p-3 bg-light border rounded-3 mt-3'),

            html.H2('Postings metadata visualisations', className='mt-3'),

            dbc.Card([
                dbc.CardHeader('Categories and technologies'),
                dbc.Row([
                    dbc.Col(graphs['categories_pie_chart'], md=6),
                    dbc.Col(graphs['technologies_pie_chart'], md=6)
                ], align='center'),
                dbc.Row([
                    dbc.Col(graphs['cat_tech_sankey_chart'], md=6),
                    dbc.Col(graphs['seniority_pie_chart'], md=6)
                ], align='center'),
            ], className=''),
            dbc.Card([
                dbc.CardHeader('Location and remote work'),
                dbc.Row([
                    dbc.Col(graphs['remote_pie_chart'], md=6),
                    dbc.Col(graphs['salaries_map'], md=6)
                ], align='center'),
                graphs['salaries_seniorities_map'],
            ], className='mt-3 '),
            dbc.Card([
                dbc.CardHeader('Salaries breakdown'),
                graphs['seniorities_histogram'],
                graphs['technologies_violin_plot'],
                graphs['contract_type_violin_plot'],
            ], className='mt-3 mb-3')
        ]),
    ], className='bg-light w-100')
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
