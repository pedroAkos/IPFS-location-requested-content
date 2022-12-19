import re

import socket
from typing import Dict, List, Any, Tuple, Union

import pandas as pd


def get_protocol_and_ip(maddr: str) -> (str, str):
    """ Extracts protocol and ip from maddr

    Parameters
    ----------
    maddr : str
        maddr

    Returns
    -------
    (str, str)
        protocol and ip
    """
    try:
        splitted = maddr.split('/')
        if 'p2p-circuit' in maddr:
            for i in range(len(splitted)):
                if splitted[i] == 'p2p':
                    return 'relay', splitted[i+1]
        proto = splitted[1]
        try:
            ip = splitted[2]
            if proto == 'ip4' or proto == 'ip6':
                return proto, ip
            elif 'dns' in proto:
                try:
                    host = socket.gethostbyname(ip)
                except:
                    return proto, None
                return proto, host
            else:
                # print(maddr)
                return proto, None
        except:
            # print(maddr)
            return proto, None
    except:
        return None, None


def extract_ips_from_maddr(maddr: str) -> (str, str):
    """ Extracts protocol and ip from maddr

    Parameters
    ----------
    maddr : str
        maddr

    Returns
    -------
    (str, str)
        protocol and ip
    """
    return get_protocol_and_ip(maddr)


def parse_providers(peers: str) -> dict[str, list[Any]]:
    """ Parses providers

    Parameters
    ----------
    peers : str
        peers

    Returns
    -------
    dict[str, str]
        parsed providers
    """
    providers = {}
    # find all matches of the regex
    for match in re.finditer('{(.*?): \[(.*?)\]}', peers):
        maddrs = []
        if match is None:  # if no match continue
            continue
        id = match.group(1).replace('{', '').replace('}', '')
        addrs = match.group(2)
        if id:  # if exists both then process addrs
            providers[id] = []
            if addrs:
                for addr in addrs.split(" "):
                    providers[id].append(addr)

    return providers


def parse_entry(line: str) -> tuple[Union[str, Any], Union[str, Any], dict[str, list[Any]], Union[str, Any]]:
    """ Parses log entry
    Parameters
    ----------
    line : str
        log entry
    Returns
    -------
    tuple[Union[str, Any], Union[str, Any], dict[str, list[Any]], Union[str, Any]]
        parsed log entry

    """
    match = re.match('(.*) Found:  (.*)  in peers:  (.*)  time:  (.*)', line)
    time = match.group(1)
    cid = match.group(2)
    peers = match.group(3)
    dur = match.group(4)
    providers = parse_providers(peers)
    return time, cid, providers, dur


def parse_failed_entry(line):
    """ Parses log entry

    Parameters
    ----------
    line : str
        log entry

    Returns
    -------
    tuple[Union[str, Any], Union[str, Any], dict[str, list[Any]], Union[str, Any]]
        parsed log entry
    """
    match = re.match('(.*) Failed:  (.*) err:  (.*)  in peers:  (.*)  time:  (.*)', line)
    time = match.group(1)
    cid = match.group(2)
    err = match.group(3)
    peers = match.group(4)
    dur = match.group(5)
    providers = parse_providers(peers)
    return time, cid, err, providers, dur