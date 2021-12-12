import numpy as np
import pandas as pd
from plotly import express as px
from plotly import graph_objects as go
from sklearn import preprocessing


class RemotePieChart:
    def make_fig(postings_df):
        fig = px.pie(postings_df, names='remote',
                     title='Fully remote possibility')
        return fig


class TechnologiesPieChart:
    N_LARGEST = 12

    def make_fig(postings_df):
        tech_n_largest = postings_df['technology'].value_counts().nlargest(
            TechnologiesPieChart.N_LARGEST).index.to_list()
        tech_n_largest_df = postings_df[postings_df['technology'].isin(
            tech_n_largest)]
        fig = px.pie(tech_n_largest_df, names='technology',
                     title='Main technology')
        return fig


class CategoriesPieChart:
    N_LARGEST = 12

    def make_fig(postings_df):
        cat_n_largest = postings_df['category'].value_counts().nlargest(
            TechnologiesPieChart.N_LARGEST).index.to_list()
        cat_n_largest_df = postings_df[postings_df['category'].isin(
            cat_n_largest)]
        fig = px.pie(cat_n_largest_df, names='category',
                     title='Main category')
        return fig


class SeniorityPieChart:
    def make_fig(seniorities_df):
        fig = px.pie(seniorities_df, names='seniority', title='Seniority')
        return fig


class CategoriesTechnologiesSankeyChart:
    N_LARGEST_CATS = 12
    N_LARGETS_TECHS = 12
    MIN_FLOW = 12

    def make_fig(postings_df):
        cat_n_largest = postings_df['category'].value_counts().nlargest(
            CategoriesTechnologiesSankeyChart.N_LARGEST_CATS).index.to_list()
        tech_n_largest = postings_df['technology'].value_counts().nlargest(
            CategoriesTechnologiesSankeyChart.N_LARGETS_TECHS).index.to_list()
        cat_n_largest_df = postings_df[postings_df['category'].isin(
            cat_n_largest)]
        cat_tech_n_largest_df = cat_n_largest_df[cat_n_largest_df['technology'].isin(
            tech_n_largest)]

        catgrp = cat_tech_n_largest_df.groupby(
            'category')['technology'].value_counts()
        catgrp = catgrp.drop(
            catgrp[catgrp < CategoriesTechnologiesSankeyChart.MIN_FLOW].index)
        catgrp = catgrp.dropna()

        catgrp_list = catgrp.index.to_list()
        sources = [el[0] for el in catgrp_list]
        targets = [el[1] for el in catgrp_list]
        values = catgrp.to_list()

        le = preprocessing.LabelEncoder()
        le.fit(sources + targets)
        sources_e = le.transform(sources)
        targets_e = le.transform(targets)

        fig = go.Figure(data=[go.Sankey(
            node=dict(
                label=np.unique(sources + targets),
            ),
            link=dict(
                source=sources_e,
                target=targets_e,
                value=values
            ))])

        return fig


class SalariesMapChart:
    N_LARGEST_CITIES = 15

    def make_fig(locations_df, salaries_df):
        loc_sal_df = locations_df.merge(salaries_df, on='id')

        job_counts = loc_sal_df.groupby('city')['id'].count()
        salaries = loc_sal_df.groupby(
            'city')[['salary_mean', 'lat', 'lon']].mean()
        cities_salaries = pd.concat(
            [job_counts.rename('job_counts'), salaries], axis=1)
        cities_salaries = cities_salaries.nlargest(
            SalariesMapChart.N_LARGEST_CITIES, 'job_counts')
        cities_salaries = cities_salaries.reset_index()
        fig = px.scatter_geo(cities_salaries, scope='europe', lat='lat', lon='lon',
                             size='job_counts', color='salary_mean', title='Salary mean and jobs number',
                             fitbounds='locations', hover_data={'city': True})
        return fig


class SalariesSeniorotiesMapChart:
    def make_fig(locations_df, salaries_df, seniorities_df):
        loc_sal_df = locations_df.merge(salaries_df, on='id')
        lss_df = loc_sal_df.merge(seniorities_df, on='id')

        lss_df = lss_df[lss_df['seniority'].isin(('Junior', 'Mid', 'Senior'))]
        salaries = lss_df.groupby(['seniority', 'city'])[
            ['salary_mean', 'lat', 'lon']].mean()
        job_counts = lss_df.groupby(['seniority', 'city'])['id'].count()
        jobs_cities_salaries = pd.concat(
            [salaries, job_counts.rename('job_counts')], axis=1).reset_index()
        fig = px.scatter_geo(jobs_cities_salaries, scope='europe', lat='lat', lon='lon',
                             size='job_counts', color='salary_mean', fitbounds='locations',
                             title='Salary mean and jobs number',
                             hover_data={'city': True}, facet_col='seniority')
        return fig


class SenioritiesHistogram:
    MAX_SALARY = 40000

    def make_fig(seniorities_df, salaries_df):
        sen_sal_df = seniorities_df.merge(salaries_df, on='id')
        sen_sal_df = sen_sal_df[sen_sal_df['salary_mean']
                                < SenioritiesHistogram.MAX_SALARY]
        sen_sal_df = sen_sal_df[sen_sal_df['salary_mean'] > 0]
        fig = px.histogram(sen_sal_df, x='salary_mean',
                           color='seniority', title='Salaries histogram')
        return fig


class ContractTypeViolinChart:
    MAX_SALARY = 40000
    TECH_N_LARGEST = 8

    def make_fig(postings_df, salaries_df):
        pos_sal_df = postings_df.merge(salaries_df, on='id')
        tech_n_largest = pos_sal_df['technology'].value_counts().nlargest(
            ContractTypeViolinChart.TECH_N_LARGEST).index.to_list()
        tech_n_largest_df = pos_sal_df[pos_sal_df['technology'].isin(
            tech_n_largest)]
        limited_salary = tech_n_largest_df[~(
            tech_n_largest_df['salary_mean'] > ContractTypeViolinChart.MAX_SALARY)]
        b2b_df = limited_salary[limited_salary['contract_type'] == 'b2b']
        perm_df = limited_salary[limited_salary['contract_type']
                                 == 'permanent']

        fig = go.Figure()
        fig.add_trace(go.Violin(x=b2b_df['technology'],
                                y=b2b_df['salary_mean'],
                                legendgroup='b2b', scalegroup='b2b', name='b2b',
                                side='negative', points=False))
        fig.add_trace(go.Violin(x=perm_df['technology'],
                                y=perm_df['salary_mean'],
                                legendgroup='permanent', scalegroup='permanent', name='permanent',
                                side='positive', points=False))
        fig.update_traces(meanline_visible=True)
        fig.update_layout(violingap=0, violinmode='overlay')
        return fig


class TechnologiesViolinChart:
    MAX_SALARY = 40000
    TECH_N_LARGEST = 8

    def make_fig(postings_df, salaries_df, senorities_df):
        pos_sal_df = postings_df.merge(salaries_df, on='id')
        pss_df = pos_sal_df.merge(senorities_df, on='id')
        tech_7_largest = pss_df['technology'].value_counts().nlargest(
            7).index.to_list()
        tech_7_largest_df = pss_df[pss_df['technology'].isin(
            tech_7_largest)]
        seniority_df = tech_7_largest_df[~(
            tech_7_largest_df['salary_mean'] > 40000)]
        seniority_df = seniority_df[seniority_df['seniority'].isin(
            ('Junior', 'Mid', 'Senior'))]
        fig = px.violin(seniority_df, y='salary_mean', x='technology',
                        color='seniority', violinmode='overlay', title='Salaries', points=False)
        return fig
