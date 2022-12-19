import re
from datetime import datetime

import dateutil.parser
import pandas as pd


def get_unique_cids(df: pd.DataFrame) -> pd.DataFrame:
    return df[['cid']].drop_duplicates()


def filter_data(df: pd.DataFrame) -> pd.DataFrame:
    # filter for GET operations
    df = df[df['op'] == 'GET']

    # filter for successful operations
    df = df.astype({'status': int})
    df = df[(df['status'] >= 200) & (df['status'] < 300)]

    # filter for remote requests
    df = df[(df['ip'] != '127.0.0.1') & (df['ip'] != '::1')]
    return df


def extract_cid(http_host: str, target: str) -> str:
    link = http_host + target
    cid = []
    cid.extend(re.findall('Qm\w+', link))
    cid.extend(re.findall('baf\w+', link))
    if len(cid) == 1:
        return cid[0]
    elif len(cid) == 0:
        return pd.NA
    else:
        ## which cid should we return ? all ?
        ## return the first for now
        return cid[0]


def extract_date(time: str) -> datetime:
    time = time.strip('][')
    return dateutil.parser.parse(time)




def parse_log_entry(logEntry: str) -> dict[str, str]:
    matches = re.findall('\"(.*?)\"', logEntry)  # finds all matches
    request = matches[0]
    http_refer = matches[1]
    http_user_agent = matches[2]
    tokens = request.split(' ')
    op = tokens[0]
    target = tokens[1]
    http = tokens[2]

    entry = re.sub('\"(.*?)\"', '', logEntry)  # substitutes all matches with '' in line
    tokens = entry.split(' ')
    i = 0
    ip = tokens[i]
    i += 3
    time = tokens[i]
    i += 2  # need to jump over 2 spaces
    status = tokens[i]
    i += 1
    body_bytes = tokens[i]
    i += 1
    request_length = tokens[i]
    i += 1
    request_time = tokens[i]
    i += 1

    upstream_response_time, upstream_header_time = [], []
    while tokens[i][-1] == ',':
        upstream_response_time.append(tokens[i][-1])
        i += 1

    upstream_response_time.append(tokens[i])
    i += 1
    while tokens[i][-1] == ',':
        upstream_header_time.append(tokens[i][-1])
        i += 1

    upstream_header_time.append(tokens[i])
    i += 1
    cache = tokens[i]
    i += 3  # need to jump over 3 spaces
    server_name = tokens[i]
    i += 1
    http_host = tokens[i]
    i += 1
    if i < len(tokens):
        scheme = tokens[i]  # [:-1] if \n in log entry
    elif 'joaoleitao' in logEntry:
        scheme = http_host
        http_host = server_name
        server_name = '*.ipfs.joaoleitao.org'


    return {'ip': ip,
            'time': time,
            'op': op,
            'target': target,
            'http': http,
            'status': status,
            'body_bytes': body_bytes,
            'request_length': request_length,
            'request_time': request_time,
            'upstream_response_time': upstream_response_time,
            'upstream_header_time': upstream_header_time,
            'cache': cache,
            'http_refer': http_refer,
            'http_user_agent': http_user_agent,
            'server_name': server_name,
            'http_host': http_host,
            'scheme': scheme}

