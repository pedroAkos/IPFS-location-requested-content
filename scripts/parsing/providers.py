import re

import socket
import pandas as pd


def extract_ips_from_maddr(peer: (str, str)) -> str:
    maddr = peer[1]
    try:
        splitted = maddr.split('/')
        proto = splitted[1]
        ip = splitted[2]
        if proto == 'ip4' or proto == 'ip6':
            return ip
        elif 'dns' in proto:
            try:
                host = socket.gethostbyname(ip)
            except:
                return pd.NA
            return host
        else:
            # print(maddr)
            return pd.NA
    except:
        # print(maddr)
        return pd.NA


def parse_providers(peers: str) -> list[(str, str)]:
    providers = []
    # find all matches of the regex
    for match in re.finditer('{(.*?): [(.*?)]}', peers):
        maddrs = []
        if match is None:  # if no match continue
            continue
        id = match.group(1)
        addrs = match.group(2)
        if id and addrs:  # if exists both then process addrs
            for addr in addrs.split(" "):
                maddrs.append(addr)
    providers.append((id, maddrs))

    return providers


def parse_entry(line: str) -> (str, str, list[(str, str)], str):
    match = re.match('(.*) Found:  (.*)  in peers:  (.*)  time:  (.*)', line)
    time = match.group(1)
    cid = match.group(2)
    peers = match.group(3)
    dur = match.group(4)
    providers = parse_providers(peers)
    providers = map(extract_ips_from_maddr, providers)
    return time, cid, providers, dur
