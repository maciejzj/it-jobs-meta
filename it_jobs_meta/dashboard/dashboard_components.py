from abc import ABC, abstractmethod
from enum import Enum, auto
from typing import Any

import numpy as np
import pandas as pd
from dash import dcc
from plotly import express as px
from plotly import graph_objects as go
from sklearn import preprocessing


def get_n_most_frequent_vals_in_col(col: pd.Series, n: int) -> list[Any]:
    return col.value_counts().nlargest(n).index.to_list()


def get_rows_with_n_most_frequent_vals_in_col(
    df: pd.DataFrame, col_name: str, n: int
) -> pd.DataFrame:
    n_most_freq = get_n_most_frequent_vals_in_col(df[col_name], n)
    return df[df[col_name].isin(n_most_freq)]


def move_legend_to_top(fig: go.Figure) -> go.Figure:
    fig.update_layout(
        legend={
            'orientation': 'h',
            'yanchor': 'bottom',
            'xanchor': 'right',
            'y': 1,
            'x': 1,
        }
    )
    return fig


def center_title(fig: go.Figure) -> go.Figure:
    fig.update_layout(title_x=0.5)
    return fig


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


class Graph(ABC):
    @classmethod
    @abstractmethod
    def make_fig(cls, postings_df: pd.DataFrame) -> go.Figure:
        pass


class GraphRegistry:
    _graph_makers: dict[Graphs, Graph] = {}

    @classmethod
    def register(cls, key):
        return lambda graph: cls._register_inner(key, graph)

    @classmethod
    def make(cls, postings_df: pd.DataFrame) -> dict[Graphs, dcc.Graph]:
        graphs: dict[Graphs, go.Figure] = {}
        for graph_key in cls._graph_makers:
            graphs[graph_key] = dcc.Graph(
                figure=cls._graph_makers[graph_key].make_fig(postings_df)
            )
        return graphs

    @classmethod
    def _register_inner(cls, key, graph):
        cls._graph_makers[key] = graph
        return graph


@GraphRegistry.register(key=Graphs.TECHNOLOGIES_PIE_CHART)
class TechnologiesPieChart(Graph):
    TITLE = 'Main technology'
    N_MOST_FREQ = 12

    @classmethod
    def make_fig(cls, postings_df: pd.DataFrame) -> go.Figure:
        tech_most_freq_df = get_rows_with_n_most_frequent_vals_in_col(
            postings_df, 'technology', cls.N_MOST_FREQ
        )

        fig = px.pie(tech_most_freq_df, names='technology', title=cls.TITLE)
        fig.update_traces(textposition='inside')
        fig = center_title(fig)
        return fig


@GraphRegistry.register(key=Graphs.CATEGORIES_PIE_CHART)
class CategoriesPieChart(Graph):
    TITLE = 'Main category'
    N_MOST_FREQ = 12

    @classmethod
    def make_fig(cls, postings_df: pd.DataFrame) -> go.Figure:
        cat_largest_df = get_rows_with_n_most_frequent_vals_in_col(
            postings_df, 'category', cls.N_MOST_FREQ
        )

        fig = px.pie(cat_largest_df, names='category', title=cls.TITLE)
        fig.update_traces(textposition='inside')
        fig = center_title(fig)
        return fig


@GraphRegistry.register(key=Graphs.CAT_TECH_SANKEY_CHART)
class CategoriesTechnologiesSankeyChart(Graph):
    TITLE = 'Categories and technologies share'
    N_MOST_FREQ_CAT = 12
    N_MOST_FREQ_TECH = 12
    MIN_FLOW = 12

    @classmethod
    def make_fig(cls, postings_df: pd.DataFrame) -> go.Figure:
        cat_most_freq = get_n_most_frequent_vals_in_col(
            postings_df['category'], cls.N_MOST_FREQ_CAT
        )
        tech_most_freq = get_n_most_frequent_vals_in_col(
            postings_df['technology'], cls.N_MOST_FREQ_TECH
        )
        cat_tech_most_freq_df = postings_df[
            postings_df['category'].isin(cat_most_freq)
            & postings_df['technology'].isin(tech_most_freq)
        ]

        catgrp = cat_tech_most_freq_df.groupby('category')[
            'technology'
        ].value_counts()
        catgrp = catgrp.drop(catgrp[catgrp < cls.MIN_FLOW].index)
        catgrp = catgrp.dropna()

        catgrp_list = catgrp.index.to_list()
        sources = [el[0] for el in catgrp_list]
        targets = [el[1] for el in catgrp_list]
        values = catgrp.to_list()

        label_encoder = preprocessing.LabelEncoder()
        label_encoder.fit(sources + targets)
        sources_e = label_encoder.transform(sources)
        targets_e = label_encoder.transform(targets)

        fig = go.Figure(
            data=[
                go.Sankey(
                    node=dict(label=np.unique(sources + targets)),
                    link=dict(source=sources_e, target=targets_e, value=values),
                )
            ]
        )
        fig.update_layout(title_text=cls.TITLE)
        fig = center_title(fig)
        return fig


@GraphRegistry.register(key=Graphs.SENIORITY_PIE_CHART)
class SeniorityPieChart(Graph):
    TITLE = 'Seniority'

    @classmethod
    def make_fig(cls, postings_df: pd.DataFrame) -> go.Figure:
        postings_df = postings_df.explode('seniority')
        fig = px.pie(postings_df, names='seniority', title=cls.TITLE)
        fig = center_title(fig)
        return fig


@GraphRegistry.register(key=Graphs.SENIORITIES_HISTOGRAM)
class SenioritiesHistogram(Graph):
    TITLE = 'Histogram'
    MAX_SALARY = 40000

    @classmethod
    def make_fig(cls, postings_df) -> go.Figure:
        postings_df = postings_df.explode('seniority')
        postings_df = postings_df[postings_df['salary_mean'] < cls.MAX_SALARY]
        postings_df = postings_df[postings_df['salary_mean'] > 0]

        fig = px.histogram(
            postings_df, x='salary_mean', color='seniority', title=cls.TITLE
        )
        fig = fig.update_layout(
            legend_title_text=None,
            xaxis_title_text='Mean salary (PLN)',
            yaxis_title_text='Count',
        )
        fig = move_legend_to_top(fig)
        return fig


@GraphRegistry.register(key=Graphs.REMOTE_PIE_CHART)
class RemotePieChart(Graph):
    TITLE = 'Fully remote work possible'

    @classmethod
    def make_fig(cls, postings_df: pd.DataFrame) -> go.Figure:
        remote_df = postings_df['remote'].replace({True: 'Yes', False: 'No'})

        fig = px.pie(remote_df, names='remote', title=cls.TITLE)
        fig = center_title(fig)
        return fig


@GraphRegistry.register(key=Graphs.SALARIES_MAP)
class SalariesMap(Graph):
    TITLE = 'Mean salary by location (PLN)'
    MIN_CITY_FREQ = 25

    @classmethod
    def make_fig(cls, postings_df) -> go.Figure:
        postings_df = postings_df.explode('city')
        postings_df[['city', 'lat', 'lon']] = postings_df['city'].transform(
            lambda city: pd.Series([city[0], city[1], city[2]])
        )

        job_counts = postings_df.groupby('city')['_id'].count()
        salaries = postings_df.groupby('city')[
            ['salary_mean', 'lat', 'lon']
        ].mean()
        cities_salaries = pd.concat(
            [job_counts.rename('job_counts'), salaries], axis=1
        )
        more_than_min = cities_salaries['job_counts'] > cls.MIN_CITY_FREQ
        cities_salaries = cities_salaries[more_than_min]
        cities_salaries = cities_salaries.reset_index()

        fig = px.scatter_geo(
            cities_salaries,
            scope='europe',
            lat='lat',
            lon='lon',
            size='job_counts',
            color='salary_mean',
            title=cls.TITLE,
            fitbounds='locations',
            labels={'salary_mean': 'Mean salary'},
            hover_data={'city': True},
        )
        fig = center_title(fig)
        return fig


class SalariesMapFilteredBySeniority(Graph):
    @classmethod
    def make_fig(
        cls,
        postings_df,
        seniority: str,
    ) -> go.Figure:

        postings_df = postings_df.explode('seniority')
        postings_df = postings_df[postings_df['seniority'] == seniority]

        fig = SalariesMap.make_fig(postings_df)
        return fig


@GraphRegistry.register(key=Graphs.SALARIES_MAP_JUNIOR)
class SalariesMapJunior(Graph):
    TITLE = 'Mean salary for Juniors'

    @classmethod
    def make_fig(
        cls,
        postings_df,
    ) -> go.Figure:

        fig = SalariesMapFilteredBySeniority.make_fig(postings_df, 'Junior')
        fig.update_layout(title=cls.TITLE)
        fig.update_coloraxes(showscale=False)
        fig = center_title(fig)
        return fig


@GraphRegistry.register(key=Graphs.SALARIES_MAP_MID)
class SalariesMapMid(Graph):
    TITLE = 'Mean salary for Mids'

    @classmethod
    def make_fig(
        cls,
        postings_df,
    ) -> go.Figure:

        fig = SalariesMapFilteredBySeniority.make_fig(postings_df, 'Mid')
        fig.update_layout(title=cls.TITLE)
        fig.update_coloraxes(showscale=False)
        fig = center_title(fig)
        return fig


@GraphRegistry.register(key=Graphs.SALARIES_MAP_SENIOR)
class SalariesMapSenior(Graph):
    TITLE = 'Mean salary for Seniors'

    @classmethod
    def make_fig(cls, postings_df) -> go.Figure:
        fig = SalariesMapFilteredBySeniority.make_fig(postings_df, 'Senior')
        fig.update_layout(title=cls.TITLE)
        fig.update_coloraxes(showscale=False)
        fig = center_title(fig)
        return fig


@GraphRegistry.register(key=Graphs.TECHNOLOGIES_VIOLIN_PLOT)
class TechnologiesViolinChart(Graph):
    TITLE = 'Salary violin plot in regard to seniority'
    MAX_SALARY = 40000
    N_MOST_FREQ_TECH = 8

    @classmethod
    def make_fig(
        cls,
        postings_df,
    ) -> go.Figure:

        postings_df = postings_df.explode('seniority')
        tech_most_freq = get_rows_with_n_most_frequent_vals_in_col(
            postings_df, 'technology', cls.N_MOST_FREQ_TECH
        )
        limited = tech_most_freq[tech_most_freq['salary_mean'] < cls.MAX_SALARY]
        limited = limited[
            limited['seniority'].isin(('Junior', 'Mid', 'Senior'))
        ]

        fig = px.violin(
            limited,
            y='salary_mean',
            x='technology',
            color='seniority',
            violinmode='overlay',
            title=cls.TITLE,
            points=False,
        )
        fig = move_legend_to_top(fig)
        fig = fig.update_traces(spanmode='hard', meanline_visible=True)
        fig = fig.update_layout(
            yaxis_title_text='Mean salary (PLN)',
            xaxis_title_text='Technology',
            legend_title_text=None,
        )
        fig = center_title(fig)
        return fig


@GraphRegistry.register(key=Graphs.CONTRACT_TYPE_VIOLIN_PLOT)
class ContractTypeViolinChart(Graph):
    TITLE = 'Salary violin plot in regard to contract'
    MAX_SALARY = 40000
    N_MOST_FREQ_TECH = 8

    @classmethod
    def make_fig(cls, postings_df) -> go.Figure:
        tech_most_freq = get_rows_with_n_most_frequent_vals_in_col(
            postings_df, 'technology', cls.N_MOST_FREQ_TECH
        )
        limited = tech_most_freq[tech_most_freq['salary_mean'] < cls.MAX_SALARY]
        b2b_df = limited[limited['contract_type'] == 'B2B']
        perm_df = limited[limited['contract_type'] == 'Permanent']

        fig = go.Figure()
        fig.add_trace(
            go.Violin(
                x=b2b_df['technology'],
                y=b2b_df['salary_mean'],
                legendgroup='B2B',
                scalegroup='B2B',
                name='B2B',
                side='negative',
                spanmode='hard',
                points=False,
            )
        )
        fig.add_trace(
            go.Violin(
                x=perm_df['technology'],
                y=perm_df['salary_mean'],
                legendgroup='Permanent',
                scalegroup='Permanent',
                name='Permanent',
                side='positive',
                spanmode='hard',
                points=False,
            )
        )
        fig.update_traces(meanline_visible=True)
        fig.update_layout(
            violingap=0,
            violinmode='overlay',
            yaxis_title_text='Mean salary (PLN)',
            xaxis_title_text='Technology',
            title=cls.TITLE,
        )
        fig = move_legend_to_top(fig)
        fig = center_title(fig)
        return fig
