from dataclasses import dataclass
from datetime import datetime

import pandera as pa


@dataclass
class Schemas:
    postings = pa.DataFrameSchema(
        {
            'id': pa.Column(str, unique=True),
            'name': pa.Column(str, nullable=True),
            'posted': pa.Column(datetime, coerce=True, nullable=True),
            'title': pa.Column(str, nullable=True),
            'technology': pa.Column(str, nullable=True),
            'category': pa.Column(str, nullable=True),
            'url': pa.Column(str, nullable=True),
            'remote': pa.Column(bool, coerce=True),
        }
    )

    salaries = pa.DataFrameSchema(
        {
            'id': pa.Column(str, unique=True),
            'contract_type': pa.Column(str),
            'salary_min': pa.Column(float, pa.Check.ge(0), coerce=True),
            'salary_max': pa.Column(float, pa.Check.ge(0), coerce=True),
            'salary_mean': pa.Column(float, pa.Check.ge(0), coerce=True),
        }
    )

    locations = pa.DataFrameSchema(
        {
            'id': pa.Column(str),
            'city': pa.Column(str),
            'lat': pa.Column(
                float, pa.Check.ge(-90), pa.Check.le(90), coerce=True
            ),
            'lon': pa.Column(
                float, pa.Check.ge(-180), pa.Check.le(180), coerce=True
            ),
        }
    )

    seniorities = pa.DataFrameSchema(
        {'id': pa.Column(str), 'seniority': pa.Column(str)}
    )
