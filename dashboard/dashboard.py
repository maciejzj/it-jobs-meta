from pathlib import Path

import dash
import pandas as pd
import sqlalchemy as db
from dash import dcc
from dash import html

from data_processing.data_warehouse import (make_db_uri_from_config,
                                            load_warehouse_db_config)
from .dashboard_components import (
    RemotePieChart,
    SalariesMapChart,
    SalariesSeniorotiesMapChart,
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



def make_layout():
    data = gather_data()
    layout = html.Div(children=[
        dcc.Graph(figure=RemotePieChart.make_fig(data['postings'])),
        dcc.Graph(figure=TechnologiesPieChart.make_fig(data['postings'])),
        dcc.Graph(figure=CategoriesPieChart.make_fig(data['postings'])),
        dcc.Graph(figure=SeniorityPieChart.make_fig(data['seniorities'])),
        dcc.Graph(figure=CategoriesTechnologiesSankeyChart.make_fig(
            data['postings'])),
        dcc.Graph(figure=SalariesMapChart.make_fig(
            data['locations'], data['salaries'])),
        dcc.Graph(figure=SalariesSeniorotiesMapChart.make_fig(
            data['locations'], data['salaries'], data['seniorities'])),
        dcc.Graph(figure=SenioritiesHistogram.make_fig(data['seniorities'], data['salaries'])),
        dcc.Graph(figure=TechnologiesViolinChart.make_fig(data['postings'], data['salaries'], data['seniorities'])),
        dcc.Graph(figure=ContractTypeViolinChart.make_fig(data['postings'], data['salaries']))
    ])
    return layout


def make_dash_app():
    app = dash.Dash('it-jobs-meta-dashboard')
    return app


if __name__ == '__main__':
    app = make_dash_app()
    app.layout = make_layout()
    app.run_server(debug=True, host='0.0.0.0')
