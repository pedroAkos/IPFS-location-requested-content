import os.path

import numpy as np
import pandas as pd
import seaborn as sns
from matplotlib import pyplot as plt
from tqdm import tqdm

import scripts.db.io as db
from scripts.db import requests, providers, queries
from scripts.plotting.colors import continent_colour


def plot_locality_heatmap(file=None, save=None, out=None):
    """ Plots the locality heatmap. """
    if os.path.isfile(file):
        df = pd.read_csv(file, index_col=0, keep_default_na=False, na_values=['', 'RL'])
    else:
        df = db.execute_query(
            f'select {requests.continent} as requester, {providers.continent} as provider, count({requests.req_id})'
            f'from {queries.requests_join_providers} '
            f'group by 1,2')
        if save:
            df.to_csv(save)

    df.fillna('Unknown', inplace=True)
    print(df)

    reqs = set(df['requester'].tolist())
    provs = set(df['provider'].tolist())
    all = sorted(reqs.union(provs))
    df.set_index(['requester', 'provider'], inplace=True)
    mat = np.zeros((len(all), len(all)), dtype=float)

    reqs = df.groupby('requester')
    i = 0
    for r in sorted(all, reverse=True):
        j = 0
        for p in all:
            try:
                mat[i, j] = float(float(sum(df.loc[(r, p)]["count"])) / float(reqs.get_group(r).sum()['count']))
                print("Added ", r, p, sum(df.loc[(r, p)]["count"]))
            except Exception as e:
                print(r, p, e)
            j += 1
        i += 1

    print(mat)
    heat = pd.DataFrame(mat)
    heat.index = sorted(all, reverse=True)
    heat.columns = all
    ax = sns.heatmap(heat, linewidths=.5, annot=True, fmt='.2%', cbar=False)
    ax.set_xlabel('Providers')
    ax.set_ylabel('Requesters')
    plt.tight_layout()
    if out:
        plt.savefig(out)
    else:
        plt.show()


def plot_asn_concentration(file=None, save=None, limit=10, out=None):
    """ Plots the number of requests originating from ASes to the ASes of providers of those requests.
    :param file: The file to load the data from.
    :param save: The file to save the data to.
    :param limit: The number of ASes to plot.
    :param out: The file to save the plot to.
    """
    if os.path.isfile(file):
        df = pd.read_csv(file, index_col=0, keep_default_na=False, na_values=['', 'RL'])
    else:
        df = db.execute_query(
            f'select {requests.asn} as requester_asn, {requests.aso} as requester_aso, '
            f'{providers.asn} as provider_asn, {providers.aso} as provider_aso, '
            f'count({requests.req_id})'
            f'from {queries.requests_join_providers} '
            f'group by 1,2,3,4')
        if save:
            df.to_csv(save)

    df.dropna(inplace=True)
    print(df)

    reqs = df.groupby(['requester_asn', 'requester_aso']).aggregate({'count': 'sum'}) \
               .sort_values(by='count', ascending=False)[:limit]

    served_reqs = df.groupby(['requester_asn', 'requester_aso']).aggregate({'count': 'max'}) \
        .sort_values(by='count', ascending=False)
    served_reqs.reset_index(inplace=True)
    top = served_reqs[served_reqs['r_asn'].isin(reqs['asn'])]

    top_reqs = df[df['r_asn'].isin(top['r_asn']) & df['count'].isin(top['count'])]
    max_reqs = df[df['r_asn'].isin(top['r_asn'])].groupby('r_asn').sum()
    top_reqs['percent'] = top_reqs.apply(lambda x: x['count'] / max_reqs.loc[x['r_asn']]['count'], axis=1)

    splots = sns.barplot(x='r_aso', y='percent', data=top_reqs)
    i = 0
    for p in splots.patches:
        splots.annotate(format(p.get_height(), '.2%'), (p.get_x() + p.get_width() / 2., p.get_height()), ha='center',
                        va='center', xytext=(0, -10), textcoords='offset points')
        splots.annotate(top_reqs.iloc[i]['p_aso'], (p.get_x() + p.get_width() / 2., p.get_height()), ha='center',
                        va='center', xytext=(0, 10), textcoords='offset points')
        i += 1
    plt.xticks(rotation=90)
    plt.xlabel('Autonomous System')
    plt.ylabel('Percentage of Requests')
    plt.tight_layout()
    for label in splots.get_xmajorticklabels():
        label.set_rotation(30)
        label.set_horizontalalignment("right")

    plt.tight_layout()
    if out:
        plt.savefig(out)
    else:
        plt.show()


def plot_request_time_by_continent(path=None, save=None, out=None):
    """ Plots the request time by continent.
    :param path: The path to the files to load the data from.
    :param save: The path to save the data to.
    :param out: The file to save the plot to.
    """
    ax = None
    for continent in tqdm(['AF', 'AS', 'EU', 'NA', 'OC', 'SA']):
        if os.path.isfile(os.path.join(path, f'{continent}.csv')):
            df = pd.read_csv(os.path.join(path, f'{continent}.csv'), index_col=0, keep_default_na=False,
                             na_values=['', 'RL'])
        else:
            df = db.execute_query(
                f'select {requests.request_time}, {requests.body_bytes}, {providers.continent} '
                f'from {queries.requests_join_providers} '
                f'where {providers.continent} = \'{continent}\' and {requests.cache} = \'MISS\'')
            if save:
                df.to_csv(os.path.join(save, f'{continent}.csv'))

        ax = sns.ecdfplot(ax=ax, data=df, x='request_time', hue='continent', log_scale=True, label=continent,
                          palette=continent_colour, legend=False)

    plt.xlabel(f'Request Time (s)')
    plt.ylabel('Proportion of requests')
    plt.tight_layout()
    plt.legend()
    if out:
        plt.savefig(out)
    else:
        plt.show()


def plot_request_time_over_body_bytes_by_continent(path=None, save=None, sample=0.1, out=None):
    """ Plots the request time over body bytes by continent.
    :param path: The path to the files to load the data from.
    :param save: The path to save the data to.
    :param sample: The proportion of the data to use to plot.
    :param out: The file to save the plot to.
    """
    fig, ax = plt.subplots(figsize=(8, 6))
    data = []
    for continent in tqdm(['AF', 'AS', 'EU', 'NA', 'OC', 'SA']):
        if os.path.isfile(os.path.join(path, f'{continent}.csv')):
            df = pd.read_csv(os.path.join(path, f'{continent}.csv'), index_col=0, keep_default_na=False,
                             na_values=['', 'RL'])
        else:
            df = db.execute_query(
                f'select {requests.request_time}, {requests.body_bytes}, {providers.continent} '
                f'from {queries.requests_join_providers} '
                f'where {providers.continent} = \'{continent}\' and {requests.cache} = \'MISS\'')
            if save:
                df.to_csv(os.path.join(save, f'{continent}.csv'))
        data.append((df.sample(frac=sample), continent))

    for df, continent in sorted(data, key=lambda x: x[0].size, reverse=True):
        ax = sns.scatterplot(ax=ax, data=df, hue='continent', x='request_time', y='body_bytes', alpha=0.1,
                             label=continent, palette=continent_colour, legend=False)

    plt.xlabel(f'Request Time (s)')
    plt.ylabel('Proportion of requests')
    plt.xscale('log')
    plt.yscale('log')
    plt.legend(bbox_to_anchor=(1.04, 1), loc='upper left', title='Continent')
    plt.tight_layout()
    if out:
        plt.savefig(out)
    else:
        plt.show()
