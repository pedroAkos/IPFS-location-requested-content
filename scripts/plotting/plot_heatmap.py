import numpy as np
import pandas as pd
import seaborn as sns

import matplotlib.pyplot as plt


def gen_matrix(df: pd.DataFrame, group_x: list[str], group_y: list[str], provs_count: pd.Series = None,
               nodes: pd.Series = None, requests: pd.Series = None):
    """ Generates a matrix of the number of requests from group_x to group_y.

    :param df: The dataframe containing the data.
    :param group_x: The columns to group by on the x axis.
    :param group_y: The columns to group by on the y axis.
    :param provs_count: The number of providers for each group on the x axis.
    :param nodes: The number of nodes for each group on the y axis.
    :param requests: The number of requests for each group on the x axis.
    :return: A matrix of the number of requests from group_x to group_y.
    """


    # print(df)
    dd = df.reset_index()
    reqs = dd.groupby(group_x).groups
    provs = dd.groupby(group_y).groups

    all = list(set(reqs.keys()).union(set(provs.keys())))
    all.sort()

    reqsMap = {}
    provsMap = {}

    i = 0
    j = len(all)-1
    for a in all:
        if a in provs.keys():
            provsMap[a] = j
        if a in reqs.keys():
            reqsMap[a] = i
        i += 1
        j -= 1

    index, columns = all, all
    if len(group_x) == 2:
        index = ['{}_{}'.format(g[0], g[1]) for g in all]
        columns = ['{}_{}'.format(g[0], g[1]) for g in all]

    mat = np.zeros((len(index), len(columns)))
    i = 0
    j = 0
    for idx, r in df.iterrows():
        r_l = None
        p_l = None
        if len(group_x) == 1:
            x1, y1 = idx
            i = reqsMap[x1]
            j = provsMap[y1]
            r_l = x1
            p_l = y1
        elif len(group_x) == 2:
            x1, x2, y1, y2 = idx
            i = reqsMap[(x1, x2)]
            j = provsMap[(y1, y2)]
            r_l = (x1, x2)
            p_l = (y1, y2)
        else:
            print("Error on", idx, r)

        count = r['count']
        if requests is not None and provs_count is not None:
            count = (count / requests.loc[r_l]['count'])
        elif requests is not None:
            count = (count / requests.loc[r_l]['count'])
        elif provs_count is not None and nodes is not None:
            count = (count / nodes.loc[r_l]['id']) / (provs_count.loc[p_l]['count'] / nodes.loc[p_l]['id'])
        elif provs_count is not None:
            # print(provsCount)
            count = count / provs_count.loc[p_l]['count']
        elif nodes is not None:
            count = (count / nodes.loc[r_l]['id']) / (nodes.loc[p_l]['id'])

        mat[j][i] = count

    heat = pd.DataFrame(mat)
    heat.index = sorted(index, reverse=True)
    heat.columns = columns
    return heat


def plot_heatmap(df: pd.DataFrame, out: str = None, group_x: list[str] = None, group_y: list[str] = None,
                 provs_count: pd.Series = None, nodes: pd.Series = None, requests: pd.Series = None):
    """ Plots a heatmap of the number of requests from group_x to group_y.

    :param df: The dataframe containing the data.
    :param out: The output file.
    :param group_x: The columns to group by on the x axis.
    :param group_y: The columns to group by on the y axis.
    :param provs_count: The number of providers for each group on the x axis.
    :param nodes: The number of nodes for each group on the y axis.
    :param requests: The number of requests for each group on the x axis.

    """

    heat = gen_matrix(df, group_x, group_y, provs_count, nodes, requests)
    ax = sns.heatmap(heat)
    ax.set_xlabel('Requesters')
    ax.set_ylabel('Providers')
    plt.tight_layout()
    if out:
        plt.savefig(out)
    else:
        plt.show()


def plot_by_continent(df: pd.DataFrame, out=None):
    """ Plots a heatmap of the number of requests from continent_request to continent_provider.

    :param df: The dataframe containing the data.
    :param out: The output file.
    """

    df = df.groupby(['continent_request', 'continent_provider']).count()
    df = df.rename(columns={'cid': 'count'})
    plot_heatmap(df, out, ['continent_request'], ['continent_provider'])


def plot_by_continent_by_requests(df: pd.DataFrame, out=None):
    """ Plots a heatmap of the number of requests from continent_request to continent_provider.

    :param df: The dataframe containing the data.
    :param out: The output file.
    """

    df = df.groupby(['continent_request', 'continent_provider']).count()
    df = df.rename(columns={'cid': 'count'})
    plot_heatmap(df, out, ['continent_request'], ['continent_provider'], requests=df.groupby('continent_request').sum())


def plot_by_country(df: pd.DataFrame, out=None):
    """ Plots a heatmap of the number of requests from country_request to country_provider.

    :param df: The dataframe containing the data.
    :param out: The output file.
    """
    df = df.groupby(['continent_request', 'country_request', 'continent_provider', 'country_provider']).count()
    df = df.rename(columns={'cid': 'count'})
    plot_heatmap(df, out, ['continent_request', 'country_request'], ['continent_provider', 'country_provider'])


def plot_by_country_by_requests(df: pd.DataFrame, out=None):
    """ Plots a heatmap of the number of requests from country_request to country_provider.

    :param df: The dataframe containing the data.
    :param out: The output file.
    """
    df = df.groupby(['continent_request', 'country_request', 'continent_provider', 'country_provider']).count()
    df = df.rename(columns={'cid': 'count'})
    plot_heatmap(df, out, ['continent_request', 'country_request'], ['continent_provider', 'country_provider'],
                 requests=df.groupby(['continent_request', 'country_request']).sum())
