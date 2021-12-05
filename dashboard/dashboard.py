from pathlib import Path

import dash
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
import sqlalchemy as db

from data_processing.data_warehouse import make_db_uri_from_config, load_warehouse_db_config


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
    layout = html.Div(children=[html.H1('Hello world!')])
    return layout


def make_dash_app():
    app=dash.Dash('it-jobs-meta-dashboard')
    return app


if __name__ == '__main__':
    data = gather_data()
    app=make_dash_app()
    app.layout=make_layout()
    app.run_server(debug=True)
