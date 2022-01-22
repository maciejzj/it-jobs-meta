from typing import Any

import numpy as np
import pandas as pd
from plotly import express as px
from plotly import graph_objects as go
from sklearn import preprocessing


def get_n_most_frequent_vals_in_col(col: pd.Series, n: int) -> list[Any]:
    return col.value_counts().nlargest(n).index.to_list()


def get_rows_with_n_most_freqent_vals_in_col(
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


class TechnologiesPieChart:
    TITLE = 'Main technology'
    N_MOST_FREQ = 12

    @classmethod
    def make_fig(cls, postings_df: pd.DataFrame) -> go.Figure:
        tech_most_freq_df = get_rows_with_n_most_freqent_vals_in_col(
            postings_df, 'technology', cls.N_MOST_FREQ
        )
        fig = px.pie(tech_most_freq_df, names='technology', title=cls.TITLE)
        fig.update_traces(textposition='inside')
        fig = center_title(fig)
        return fig


class CategoriesPieChart:
    TITLE = 'Main category'
    N_MOST_FREQ = 12

    @classmethod
    def make_fig(cls, postings_df: pd.DataFrame) -> go.Figure:
        cat_largest_df = get_rows_with_n_most_freqent_vals_in_col(
            postings_df, 'category', cls.N_MOST_FREQ
        )
        fig = px.pie(cat_largest_df, names='category', title=cls.TITLE)
        fig.update_traces(textposition='inside')
        fig = center_title(fig)
        return fig


class CategoriesTechnologiesSankeyChart:
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


class SeniorityPieChart:
    TITLE = 'Seniority'

    @classmethod
    def make_fig(cls, seniorities_df: pd.DataFrame) -> go.Figure:
        fig = px.pie(seniorities_df, names='seniority', title=cls.TITLE)
        fig = center_title(fig)
        return fig


class SenioritiesHistogram:
    TITLE = 'Histogram'
    MAX_SALARY = 40000

    @classmethod
    def make_fig(
        cls, seniorities_df: pd.DataFrame, salaries_df: pd.DataFrame
    ) -> go.Figure:
        sen_sal_df = seniorities_df.merge(salaries_df, on='id')
        sen_sal_df = sen_sal_df[sen_sal_df['salary_mean'] < cls.MAX_SALARY]
        sen_sal_df = sen_sal_df[sen_sal_df['salary_mean'] > 0]
        fig = px.histogram(
            sen_sal_df, x='salary_mean', color='seniority', title=cls.TITLE
        )
        fig = fig.update_layout(
            legend_title_text=None,
            xaxis_title_text='Mean salary (PLN)',
            yaxis_title_text='Count',
        )
        fig = move_legend_to_top(fig)
        return fig


class RemotePieChart:
    TITLE = 'Fully remote work possible'

    @classmethod
    def make_fig(cls, postings_df: pd.DataFrame) -> go.Figure:
        remote_df = postings_df['remote'].replace({1: 'Yes', 0: 'No'})
        fig = px.pie(remote_df, names='remote', title=cls.TITLE)
        fig = center_title(fig)
        return fig


class SalariesMap:
    TITLE = 'Mean salary by location (PLN)'
    MIN_CITY_FREQ = 15

    @classmethod
    def make_fig(
        cls, locations_df: pd.DataFrame, salaries_df: pd.DataFrame
    ) -> go.Figure:

        loc_sal_df = locations_df.merge(salaries_df, on='id')
        job_counts = loc_sal_df.groupby('city')['id'].count()
        job_counts = job_counts.rename('job_counts')

        salaries = loc_sal_df.groupby('city')[['salary_mean', 'lat', 'lon']]
        salaries = salaries.mean()
        cities_salaries = pd.concat([job_counts, salaries], axis=1)
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


class SalariesMapFilteredBySeniority:
    @classmethod
    def make_fig(
        cls,
        locations_df: pd.DataFrame,
        salaries_df: pd.DataFrame,
        seniorities_df: pd.DataFrame,
        seniority: str,
    ) -> go.Figure:

        loc_sen_df = locations_df.merge(seniorities_df, on='id')
        loc_sen_df = loc_sen_df[loc_sen_df['seniority'] == seniority]
        return SalariesMap.make_fig(loc_sen_df, salaries_df)


class SalariesMapJunior:
    TITLE = 'Mean salary for Juniors'

    @classmethod
    def make_fig(
        cls,
        locations_df: pd.DataFrame,
        salaries_df: pd.DataFrame,
        seniorities_df: pd.DataFrame,
    ) -> go.Figure:

        fig = SalariesMapFilteredBySeniority.make_fig(
            locations_df, salaries_df, seniorities_df, 'Junior'
        )
        fig.update_layout(title=cls.TITLE)
        fig.update_coloraxes(showscale=False)
        fig = center_title(fig)
        return fig


class SalariesMapMid:
    TITLE = 'Mean salary for Mids'

    @classmethod
    def make_fig(
        cls,
        locations_df: pd.DataFrame,
        salaries_df: pd.DataFrame,
        seniorities_df: pd.DataFrame,
    ) -> go.Figure:

        fig = SalariesMapFilteredBySeniority.make_fig(
            locations_df, salaries_df, seniorities_df, 'Mid'
        )
        fig.update_layout(title=cls.TITLE)
        fig.update_coloraxes(showscale=False)
        fig = center_title(fig)
        return fig


class SalariesMapSenior:
    TITLE = 'Mean salary for Seniors'

    @classmethod
    def make_fig(
        cls,
        locations_df: pd.DataFrame,
        salaries_df: pd.DataFrame,
        seniorities_df: pd.DataFrame,
    ) -> go.Figure:

        fig = SalariesMapFilteredBySeniority.make_fig(
            locations_df, salaries_df, seniorities_df, 'Senior'
        )
        fig.update_layout(title=cls.TITLE)
        fig.update_coloraxes(showscale=False)
        fig = center_title(fig)
        return fig


class SalariesSenioritiesMapChart:
    TITLE = 'Mean salary and number of job offers'
    MIN_CITY_FREQ = 10

    @classmethod
    def make_fig(
        cls,
        locations_df: pd.DataFrame,
        salaries_df: pd.DataFrame,
        seniorities_df: pd.DataFrame,
    ) -> go.Figure:

        loc_sal_df = locations_df.merge(salaries_df, on='id')
        lss_df = loc_sal_df.merge(seniorities_df, on='id')

        lss_df = lss_df[lss_df['seniority'].isin(('Junior', 'Mid', 'Senior'))]
        salaries = lss_df.groupby(['seniority', 'city'])[
            ['salary_mean', 'lat', 'lon']
        ].mean()
        job_counts = lss_df.groupby(['seniority', 'city'])['id'].count()
        jobs_cities_salaries = pd.concat(
            [salaries, job_counts.rename('job_counts')], axis=1
        ).reset_index()
        fig = px.scatter_geo(
            jobs_cities_salaries,
            scope='europe',
            lat='lat',
            lon='lon',
            size='job_counts',
            color='salary_mean',
            fitbounds='locations',
            title='Salary mean and jobs number',
            hover_data={'city': True},
            facet_col='seniority',
        )
        fig = move_legend_to_top(fig)
        fig = center_title(fig)
        return fig


class TechnologiesViolinChart:
    TITLE = 'Salary violin plot in regard to senioroty'
    MAX_SALARY = 40000
    N_MOST_FREQ_TECH = 8

    @classmethod
    def make_fig(
        cls,
        postings_df: pd.DataFrame,
        salaries_df: pd.DataFrame,
        senorities_df: pd.DataFrame,
    ) -> go.Figure:

        pos_sal_df = postings_df.merge(salaries_df, on='id')
        pss_df = pos_sal_df.merge(senorities_df, on='id')
        tech_most_freq = get_rows_with_n_most_freqent_vals_in_col(
            pss_df, 'technology', cls.N_MOST_FREQ_TECH
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


class ContractTypeViolinChart:
    TITLE = 'Salary violin plot in regard to contract'
    MAX_SALARY = 40000
    N_MOST_FREQ_TECH = 8

    @classmethod
    def make_fig(
        cls, postings_df: pd.DataFrame, salaries_df: pd.DataFrame
    ) -> go.Figure:
        pos_sal_df = postings_df.merge(salaries_df, on='id')
        tech_most_freq = get_rows_with_n_most_freqent_vals_in_col(
            pos_sal_df, 'technology', cls.N_MOST_FREQ_TECH
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
