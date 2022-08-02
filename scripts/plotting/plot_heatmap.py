import numpy as np
import pandas as pd
import seaborn as sns

import matplotlib.pyplot as plt


def genMatrix(df: pd.DataFrame, groupX: list[str], groupY: list[str], provsCount: pd.Series = None,
              nodes: pd.Series = None, requests: pd.Series = None):
    # print(df)
    dd = df.reset_index()
    reqs = dd.groupby(groupX).groups
    provs = dd.groupby(groupY).groups

    all = list(set(reqs.keys()).union(set(provs.keys())))
    all.sort()

    reqsMap = {}
    provsMap = {}

    i = 0
    for a in all:
        if a in provs.keys():
            provsMap[a] = i
        if a in reqs.keys():
            reqsMap[a] = i
        i += 1

    index = ['{}{}'.format(g[0], g[1]) for g in all]
    columns = ['{}{}'.format(g[0], g[1]) for g in all]

    mat = np.zeros((len(index), len(columns)))
    i = 0
    j = 0
    for idx, r in df.iterrows():
        r_l = None
        p_l = None
        if len(groupX) == 1:
            x1, y1 = idx
            i = reqsMap[x1]
            j = provsMap[y1]
            r_l = x1
            p_l = y1
        elif len(groupX) == 2:
            x1, x2, y1, y2 = idx
            i = reqsMap[(x1, x2)]
            j = provsMap[(y1, y2)]
            r_l = (x1, x2)
            p_l = (y1, y2)
        else:
            print("Error on", idx, r)

        count = r['count']
        if requests is not None and provsCount is not None:
            count = (count / requests.loc[r_l]['count'])
        elif requests is not None:
            count = (count / requests.loc[r_l]['count'])
        elif provsCount is not None and nodes is not None:
            count = (count / nodes.loc[r_l]['id']) / (provsCount.loc[p_l]['count'] / nodes.loc[p_l]['id'])
        elif provsCount is not None:
            # print(provsCount)
            count = count / provsCount.loc[p_l]['count']
        elif nodes is not None:
            count = (count / nodes.loc[r_l]['id']) / (nodes.loc[p_l]['id'])

        mat[i][j] = count

    heat = pd.DataFrame(mat)
    heat.index = index
    heat.columns = columns
    return heat


def plot_heatmap(df: pd.DataFrame, out: str = None, groupX: list[str] = None, groupY: list[str] = None,
                 provsCount: pd.Series = None, nodes: pd.Series = None, requests: pd.Series = None):
    heat = genMatrix(df, groupX, groupY, provsCount, nodes, requests)
    ax = sns.heatmap(heat)
    ax.set_xlabel('Requesters')
    ax.set_ylabel('Providers')
    plt.tight_layout()
    if out:
        plt.savefig(out)
    else:
        plt.show()


def plot_by_continent(df: pd.DataFrame, out=None):
    plot_heatmap(df, out, ['continent_request'], ['continent_provider'])


def plot_by_continent_by_requests(df: pd.DataFrame, out=None):
    plot_heatmap(df, out, ['continent_request'], ['continent_provider'], requests=df.groupby('continent_request').sum())


def plot_by_country(df: pd.DataFrame, out=None):
    plot_heatmap(df, out, ['continent_request', 'country_request'], ['continent_provider', 'country_provider'])


def plot_by_country_by_requests(df: pd.DataFrame, out=None):
    plot_heatmap(df, out, ['continent_request', 'country_request'], ['continent_provider', 'country_provider'],
                 requests=df.groupby(['continent_request', 'country_request']).sum())
