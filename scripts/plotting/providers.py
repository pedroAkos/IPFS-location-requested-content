import os.path

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

import scripts.db.io as db
from scripts.db import providers

from scripts.plotting.colors import continent_colour


def get_cid_replicas():
    """ Returns a dataframe with the number of replicas per cid. """
    df = db.execute_query(f'select {providers.cid}, count(distinct {providers.peerID}) '
                             f'from {providers.Table} '
                             f'group by 1 '
                             f'order by 2 desc')
    return df


def get_cid_replicas_per_continent():
    """ Returns a dataframe with the number of replicas per cid per continent. """
    df = db.execute_query(f'select {providers.continent}, {providers.cid}, count(distinct {providers.peerID}) '
                             f'from {providers.Table} '
                             f'group by 1,2 '
                             f'order by 3 desc')
    return df


def get_providers_cids():
    """ Returns a dataframe with the number of cids per provider. """
    df = db.execute_query(f'select {providers.peerID}, count({providers.cid}) '
                             f'from {providers.Table} '
                             f'group by 1 '
                             f'order by 2 desc')
    return df


def get_providers_cids_per_continent():
    """ Returns a dataframe with the number of cids per provider per continent. """
    df = db.execute_query(f'select {providers.continent}, {providers.peerID}, count({providers.cid}) '
                             f'from {providers.Table} '
                             f'group by 1,2 '
                             f'order by 3 desc')
    return df


def plot_cid_replicas(file=None, save=None, out=None, ylim=None):
    """ Plots the number of replicas per cid. """
    if os.path.isfile(file):
        df = pd.read_csv(file, index_col=0, keep_default_na=False)
    else:
        df = get_cid_replicas()
        if save:
            df.to_csv(save)

    sns.ecdfplot(df, x="count", log_scale=True)
    if ylim:
        plt.ylim(ylim)
    plt.ylabel('Proportion of cIds')
    plt.xlabel('Replicas')
    plt.tight_layout()
    if out:
        plt.savefig(out)
    else:
        plt.show()
    plt.close()


def plot_cid_replicas_per_continent(file=None, save=None, out=None, ylim=None):
    """ Plots the number of replicas per cid per continent.
    :param file: The file to load the data from.
    :param save: The file to save the data to.
    :param out: The file to save the plot to.
    :param ylim: The y-axis limits.
    """
    if os.path.isfile(file):
        df = pd.read_csv(file, index_col=0, keep_default_na=False, na_values=['', 'RL'])
    else:
        df = get_cid_replicas_per_continent()
        if save:
            df.to_csv(save)

    df.fillna('Unknown', inplace=True)
    #df[df['continent'] == 'Unknown']['count'] += df.loc[df['continent'] == 'RL']['count']
    print(df)
    sns.ecdfplot(df, x="count", hue='continent', palette=continent_colour, log_scale=True)
    if ylim:
        plt.ylim(ylim)
    plt.ylabel('Proportion of cIds')
    plt.xlabel('Replicas')
    plt.tight_layout()
    if out:
        plt.savefig(out)
    else:
        plt.show()
    plt.close()


def plot_cids_per_provider(file=None, save=None, out=None, ylim=None):
    """ Plots the number of cids per provider.
    :param file: The file to load the data from.
    :param save: The file to save the data to.
    :param out: The file to save the plot to.
    :param ylim: The y-axis limits.
    """
    if os.path.isfile(file):
        df = pd.read_csv(file, index_col=0, keep_default_na=False, na_values=['', 'RL'])
    else:
        df = get_providers_cids()
        if save:
            df.to_csv(save)

    df.fillna('Unknown', inplace=True)
    print(df)
    sns.ecdfplot(df, x="count", log_scale=True)
    if ylim:
        plt.ylim(ylim)
    plt.ylabel('Proportion of providers')
    plt.xlabel('cIds per provider')
    plt.tight_layout()
    if out:
        plt.savefig(out)
    else:
        plt.show()
    plt.close()


def plot_cids_per_provider_per_continent(file=None, save=None, out=None, ylim=None):
    """ Plots the number of cids per provider per continent.
    :param file: The file to load the data from.
    :param save: The file to save the data to.
    :param out: The file to save the plot to.
    :param ylim: The y-axis limits.
    """
    if os.path.isfile(file):
        df = pd.read_csv(file, index_col=0, keep_default_na=False, na_values=['', 'RL'])
    else:
        df = get_providers_cids_per_continent()
        if save:
            df.to_csv(save)

    df.fillna('Unknown', inplace=True)
    print(df)
    sns.ecdfplot(df, x="count", hue='continent', palette=continent_colour, log_scale=True)
    if ylim:
        plt.ylim(ylim)
    plt.ylabel('Proportion of providers')
    plt.xlabel('cIds per provider')
    plt.tight_layout()
    if out:
        plt.savefig(out)
    else:
        plt.show()
    plt.close()


def get_asn_concentration():
    df = db.io.execute_query(
        f"select {providers.asn}, {providers.aso}, count({providers.asn}) as count "
        f"from {providers.Table} "
        f"group by {providers.asn} "
        f"order by count desc")
    return df


def plot_asn_concentration(file=None, save=None, out=None):
    """ Plot the concentration of ASNs over the providers
    :param file: The file to load the data from
    :param save: The file to save the data to
    :param out: The file to save the plot to
    """
    if os.path.isfile(file):
        df = pd.read_csv(file, index_col=0, keep_default_na=False)
    else:
        df = get_asn_concentration()
        if save:
            df.to_csv(save)
    print(df)

    df = df.groupby('count').count()
    df.reset_index(inplace=True)

    df.plot.scatter('asn', 'count', logx=False, logy=True)
    plt.xlabel('Number of ASes')
    plt.ylabel(f'Frequency of requests')
    plt.tight_layout()
    if out:
        plt.savefig(out)
    else:
        plt.show()
    plt.close()