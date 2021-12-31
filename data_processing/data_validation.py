from datetime import datetime

import pandera as pa
from pandera import typing as pt


class PostingsSchema(pa.SchemaModel):
    id: pt.Series[str] = pa.Field(unique=True)
    name: pt.Series[str] = pa.Field(nullable=True)
    posted:  pt.Series[datetime] = pa.Field(coerce=True, nullable=True)
    title: pt.Series[str] = pa.Field(nullable=True)
    technology: pt.Series[str] = pa.Field(nullable=True)
    category: pt.Series[str] = pa.Field(nullable=True)
    url: pt.Series[str] = pa.Field(nullable=True)
    remote: pt.Series[bool] = pa.Field(coerce=True)


class SalariesSchema(pa.SchemaModel):
    id: pt.Series[str] = pa.Field(unique=True)
    contract_type: pt.Series[str] = pa.Field()
    salary_min: pt.Series[float] = pa.Field(coerce=True, ge=0)
    salary_max: pt.Series[float] = pa.Field(coerce=True, ge=0)
    salary_mean: pt.Series[float] = pa.Field(coerce=True, ge=0)


class LocationsSchema(pa.SchemaModel):
    id: pt.Series[str] = pa.Field()
    city: pt.Series[str] = pa.Field()
    lat: pt.Series[float] = pa.Field(coerce=True, ge=-90, le=90)
    lon: pt.Series[float] = pa.Field(coerce=True, ge=-180, le=180)


class SenioritiesSchema(pa.SchemaModel):
    id: pt.Series[str] = pa.Field()
    seniority: pt.Series[str] = pa.Field(
        isin=('Trainee', 'Junior', 'Mid', 'Senior', 'Expert'))
