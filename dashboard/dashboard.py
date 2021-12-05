from pathlib import Path

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


if __name__ == '__main__':
    data = gather_data()
