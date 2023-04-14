import os.path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns

import scripts.db as db
from scripts.db import requests, queries, providers

from scripts.plotting.colors import continent_colour


def get_request_over_time(time_unit):
    """ Get the number of requests per time unit
    :param time_unit: The time unit to group by
    """
    df = db.io.execute_query(
        f'select date_trunc(\'{time_unit}\', {requests.timestamp}) as "Time", count({requests.req_id}) as "Requests per {time_unit}"'
        f'from {requests.Table} '
        f'group by 1')
    return df


def find_night_hours(datetime_array):
    """ Find the indices of the night hours in a datetime array
    :param datetime_array: The datetime array to search
    :return: The indices of the night hours
    """
    indices = []
    timezone = 0
    for i in range(len(datetime_array)):
        if 7 > datetime_array[i].hour or datetime_array[i].hour > 19:
            if 0 < i + timezone < len(datetime_array):
                indices.append((i + timezone))
    return indices


def highlight_datetimes(df, indices, ax):
    """ Highlight the given indices in the given dataframe
    :param df: The dataframe to highlight
    :param indices: The indices to highlight
    :param ax: The axis to highlight on
    """
    i = 0
    while i < len(indices) - 1:
        ax.axvspan(df.index[indices[i]], df.index[indices[i] + 1], facecolor='gray', edgecolor='none', alpha=.5)
        i += 1


def plot_requests_over_time(time_unit, file=None, save=None, out=None):
    """ Plot the number of requests over time
    :param time_unit: The time unit to group by
    :param file: The file to load the data from
    :param save: The file to save the data to
    :param out: The file to save the plot to
    """
    if os.path.isfile(file):
        df = pd.read_csv(file, index_col=0, keep_default_na=False)
        df.index = pd.to_datetime(df.index)
    else:
        df = get_request_over_time(time_unit)
        df.set_index(["Time"], inplace=True)
        if save:
            df.to_csv(save)
    print(df)
    ax = df.plot(legend=False, figsize=(12, 4), x_compat=True)
    if time_unit == 'hour':
        ax.xaxis.set_major_locator(mdates.DayLocator())
    elif time_unit == 'minute':
        ax.xaxis.set_major_locator(mdates.HourLocator())
    elif time_unit == 'second':
        ax.xaxis.set_major_locator(mdates.MinuteLocator())
    else:
        raise ValueError(f'Invalid time unit: {time_unit}')
    plt.xlabel('Time')
    plt.ylabel(f'Requests per {time_unit}')
    plt.tight_layout()
    if out is None:
        plt.show()
    else:
        plt.savefig(out)
    plt.close()


def get_requests_over_time_by_continent(time_unit):
    """ Get the number of requests per time unit by continent
    :param time_unit: The time unit to group by
    """
    df = db.io.execute_query(
        f'select date_trunc(\'{time_unit}\', {requests.timestamp}) as "Time", {requests.continent}, count({requests.req_id}) as requests '
        f'from {requests.Table} '
        f'group by 1,2')
    df.set_index(["Time"], inplace=True)
    # times = [str(t) for t in df.index.tolist()]
    times = df.index.tolist()
    df.reset_index(drop=False, inplace=True)
    continents = df.groupby("continent").groups
    df.set_index(["Time", "continent"], inplace=True)
    print(df)
    d = {}
    for c in continents:
        c_line = np.zeros(len(times))
        i = 0
        for t in times:
            try:
                c_line[i] = df.loc[(str(t), c)]["requests"]
            except:
                pass
            i += 1
        d[c] = c_line
    df = pd.DataFrame(d, index=times)
    return df


def plot_requests_over_time_by_continent(time_unit, file=None, save=None, out=None, days=None, start='7'):
    """ Plot the number of requests over time by continent
    :param time_unit: The time unit to group by
    :param file: The file to load the data from
    :param save: The file to save the data to
    :param out: The file to save the plot to
    :param days: The number of days to plot
    :param start: The start date to plot
    """
    if os.path.isfile(file):
        df = pd.read_csv(file, index_col=0, keep_default_na=False)
        df.index = pd.to_datetime(df.index)
    else:
        df = get_requests_over_time_by_continent(time_unit)
        if save:
            df.to_csv(save)

    if days is None:
        ax = df.plot(figsize=(12, 4))
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))
    else:
        df = df.loc[
             pd.to_datetime(f'2022-03-{start}'):pd.to_datetime(f'2022-03-{start}') + pd.Timedelta(days, unit='d')]
        ax = df.plot(figsize=(12, 4))
        ax.xaxis.set_major_locator(mdates.HourLocator(interval=2))
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
        night = find_night_hours(df.index)
        highlight_datetimes(df, night, ax)

        plt.title(f'2022-03-{start} to {(pd.to_datetime(f"2022-03-{start}") + pd.Timedelta(days, unit="d")).date()}')
    plt.xlabel('Time')
    plt.ylabel(f'Requests per {time_unit}')
    plt.tight_layout()
    if out is None:
        plt.show()
    else:
        plt.savefig(out)
    plt.close()


def get_cid_popularity():
    """ Get the popularity of each CID """

    df = db.io.execute_query(
        f'select count as frequency, count(cid) as occurrences '
        f'from (select {requests.cid}, count({requests.req_id})'
        f'from {requests.Table} '
        f'group by 1 '
        f'order by 2 desc) as reqcount '
        f'group by 1')
    return df


def get_cid_popularity_dist():
    """ Get the popularity distribution of each CID """
    df = db.io.execute_query(
        f'select {requests.cid}, count({requests.req_id}) as frequency '
        f'from {requests.Table} '
        f'group by 1 '
        f'order by 2 desc'
    )
    return df


def get_cid_popularity_by_continent_dist():
    """ Get the popularity distribution of each CID by continent """
    df = db.io.execute_query(
        f'select {requests.continent}, {requests.cid}, count({requests.req_id}) as frequency '
        f'from {requests.Table} '
        f'group by 1,2 '
        f'order by 3 desc'
    )
    return df


def get_cid_popularity_by_continent():
    """ Get the popularity of each CID by continent """
    df = db.io.execute_query(
        f'select continent, count as frequency, count(cid) as occurrences '
        f'from (select {requests.continent}, {requests.cid}, count({requests.req_id})'
        f'from {requests.Table} '
        f'group by 1,2 '
        f'order by 3 desc) as reqcount '
        f'group by 1,2')
    return df


def get_cid_popularity_by_provider_continent():
    """ Get the popularity of each CID by provider and continent """
    df = db.io.execute_query(
        f'select continent, count as frequency, count(cid) as occurrences '
        f'from (select {providers.continent}, {providers.cid}, count({requests.req_id})'
        f'from {queries.requests_join_providers} '
        f'group by 1,2 '
        f'order by 3 desc) as reqcount '
        f'group by 1,2')
    return df


def plot_cid_popularity_by_continent(file=None, save=None, out=None):
    """ Plot the popularity of each CID by continent"""
    if os.path.isfile(file):
        df = pd.read_csv(file, index_col=0, keep_default_na=False)
    else:
        df = get_cid_popularity_by_continent()
    print(df)
    if save:
        df.to_csv(save)
    groups = df.groupby('continent')
    ax = None
    for g in groups.groups:
        ax = groups.get_group(g).plot.scatter('occurrences', 'frequency', c=continent_colour[g], ax=ax, label=g,
                                              logx=True, logy=True)
    plt.xlabel('Occurrences of cIds')
    plt.ylabel(f'Frequency of requests')
    plt.tight_layout()
    if out:
        plt.savefig(out)
    else:
        plt.show()
    plt.close()


def plot_cid_popularity(file=None, save=None, out=None):
    """ Plot the popularity of each CID
    :param file: The file to load the data from
    :param save: The file to save the data to
    :param out: The file to save the plot to

    """
    if os.path.isfile(file):
        df = pd.read_csv(file, index_col=0, keep_default_na=False)
    else:
        df = get_cid_popularity()
        if save:
            df.to_csv(save)
    # df.set_index('occurrence', inplace=True)
    print(df)

    df.plot.scatter('occurrences', 'frequency', logx=True, logy=True)
    plt.xlabel('Occurrences of cIds')
    plt.ylabel(f'Frequency of requests')
    plt.tight_layout()
    if out:
        plt.savefig(out)
    else:
        plt.show()
    plt.close()


def plot_cid_popularity_dist(file=None, save=None, out=None):
    """ Plot the popularity distribution of each CID
    :param file: The file to load the data from
    :param save: The file to save the data to
    :param out: The file to save the plot to
    """
    if os.path.isfile(file):
        df = pd.read_csv(file, index_col=0, keep_default_na=False)
    else:
        df = get_cid_popularity_dist()
        if save:
            df.to_csv(save)
    # df.set_index('occurrence', inplace=True)
    df.reset_index(inplace=True)
    print(df)

    df.plot.bar(x='index', y='count')
    # sns.displot(df, y='count', log_scale=True)
    plt.xlabel(f'Frequency of requests')
    plt.ylabel('cIds')
    plt.tight_layout()
    if out:
        plt.savefig(out)
    else:
        plt.show()
    plt.close()


def plot_cid_popularity_ecdf(file=None, save=None, out=None):
    """ Plot the popularity distribution of each CID
    :param file: The file to load the data from
    :param save: The file to save the data to
    :param out: The file to save the plot to
    """
    if os.path.isfile(file):
        df = pd.read_csv(file, index_col=0, keep_default_na=False)
    else:
        df = get_cid_popularity_dist()
        if save:
            df.to_csv(save)
    # df.set_index('occurrence', inplace=True)
    print(df)

    sns.ecdfplot(df, x='frequency', log_scale=True)
    plt.xlabel(f'Frequency of requests')
    plt.ylabel('Proportion of cIds')
    plt.tight_layout()
    if out:
        plt.savefig(out)
    else:
        plt.show()
    plt.close()


def plot_cid_popularity_by_continent_ecdf(file=None, save=None, out=None):
    """ Plot the popularity distribution of each CID by continent
    :param file: The file to load the data from
    :param save: The file to save the data to
    :param out: The file to save the plot to
    """
    if os.path.isfile(file):
        df = pd.read_csv(file, index_col=0, keep_default_na=False)
    else:
        df = get_cid_popularity_by_continent_dist()
        if save:
            df.to_csv(save)
    print(df)

    sns.ecdfplot(df, hue='continent', palette=continent_colour, x='frequency', log_scale=True)
    plt.xlabel(f'Frequency of requests')
    plt.ylabel('Proportion of cIds')
    plt.tight_layout()
    if out:
        plt.savefig(out)
    else:
        plt.show()
    plt.close()


def plot_cid_popularity_by_provider_continent_ecdf(file=None, save=None, out=None):
    """ Plot the popularity distribution of each CID by provider and continent
    :param file: The file to load the data from
    :param save: The file to save the data to
    :param out: The file to save the plot to
    """
    if os.path.isfile(file):
        df = pd.read_csv(file, index_col=0, keep_default_na=False)
    else:
        df = get_cid_popularity_by_provider_continent()
        if save:
            df.to_csv(save)
    print(df)

    sns.ecdfplot(df, hue='continent', palette=continent_colour, x='frequency', log_scale=True)
    plt.xlabel(f'Frequency of requests')
    plt.ylabel('Proportion of cIds')
    plt.tight_layout()
    if out:
        plt.savefig(out)
    else:
        plt.show()
    plt.close()


def get_asn_concentration():
    df = db.io.execute_query(
        f"select {requests.asn}, {requests.aso}, count({requests.asn}) as count "
        f"from {requests.Table} "
        f"group by {requests.asn} "
        f"order by count desc")
    return df


def plot_asn_concentration(file=None, save=None, out=None):
    """ Plot the concentration of ASNs over the requests
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


def get_request_time():
    df = db.io.execute_query(
        f"select {requests.request_time}, {requests.body_bytes}, {requests.cache} "
        f"from {requests.Table}")
    return df


def plot_request_time(file=None, save=None, out=None, hue=None):
    """ Plot the distribution of the time of the requests
    :param file: The file to load the data from
    :param save: The file to save the data to
    :param out: The file to save the plot to
    :param hue: The column to use for the hue
    """
    if os.path.isfile(file):
        df = pd.read_csv(file, index_col=0, keep_default_na=False)
    else:
        df = get_request_time()
        if save:
            df.to_csv(save)
    print(df)

    sns.ecdfplot(df, x='request_time', hue=hue, log_scale=True)
    plt.xlabel(f'Request Time (s)')
    plt.ylabel('Proportion of requests')
    plt.tight_layout()
    if out:
        plt.savefig(out)
    else:
        plt.show()
    plt.close()


def plot_request_time_over_body_bytes(file=None, save=None, out=None, cache=None):
    """Plot the distribution of request time over body bytes
    :param file: The file to load the data from
    :param save: The file to save the data to
    :param out: The fle to save the plot to
    :param cache: The cache status to plot
    """

    if os.path.isfile(file):
        df = pd.read_csv(file, index_col=0, keep_default_na=False)
    else:
        df = get_request_time()
        if save:
            df.to_csv(save)

    if cache is not None:
        df = df[df['cache'] == cache]

    sns.scatterplot(data=df, x='request_time', y='body_bytes', alpha=0.3)
    plt.xlabel(f'Request Time (s)')
    plt.ylabel('Body Bytes')
    plt.xscale('log')
    plt.yscale('log')
    plt.tight_layout()