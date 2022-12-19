import glob
import os
import sys

from multiprocessing import Pool

import pandas as pd
from tqdm import tqdm

from scripts.parsing import location, providers

# Extracts statistical information from the logs of the find providers process.

def get_stats(file):
    cids_with_prov = 0
    cids_without_prov = 0
    distinct_prov = set()
    distinct_relayed_prov = set()
    distinct_prov_with_addr = set()
    distinct_prov_without_addr = set()
    distinct_prov_with_location = set()
    distinct_prov_without_location = set()
    addr_type = {}
    dns_peers = {}
    with open(file, 'rt') as f, open(f'providers_no_addr.{os.path.basename(file)}', 'w') as w:
        for line in tqdm(f, desc=f'Processing file {file}'):
            if "Found" in line:
                time, cid, provs, dur = providers.parse_entry(line)

                if not provs:
                    cids_without_prov += 1
                else:
                    cids_with_prov += 1
                    distinct_prov = distinct_prov.union(provs.keys())
                    for pid, addrs in provs.items():
                        if not addrs:
                            distinct_prov_without_addr.add(pid)
                            w.write(f'{cid} {pid}\n')
                        else:
                            distinct_prov_with_addr.add(pid)
                            for addr in addrs:
                                proto, ip = providers.get_protocol_and_ip(addr)
                                if ip is None or ip == '127.0.0.1' or ip == '::1':
                                    continue
                                if proto not in addr_type:
                                    addr_type[proto] = 0
                                addr_type[proto] += 1
                                if 'dns' in proto:
                                    if pid not in dns_peers:
                                        dns_peers[pid] = set()
                                    dns_peers[pid].add(addr)
                                if proto == 'relay':
                                    distinct_relayed_prov.add(pid)
                                    # print('relay', pid, addr)
                                else:
                                    continent, country, region, latitude, longitude, asn, aso = location.lookup_geoip2(
                                        ip)
                                    if continent is not None:
                                        distinct_prov_with_location.add(pid)
                                        # print('loc', pid, addr)
                                    else:
                                        distinct_prov_without_location.add(pid)
                                        # print('no loc', pid, addr)

            elif "Failed" in line:
                time, cid, err, provs, dur = providers.parse_failed_entry(line)
                for pid in provs.keys():
                    distinct_prov_without_addr.add(pid)

    return dns_peers, cids_with_prov, cids_without_prov, distinct_prov, distinct_relayed_prov, distinct_prov_with_addr, distinct_prov_without_addr, distinct_prov_with_location, distinct_prov_without_location, addr_type


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print('Usage: {} <pattern for files to process> <output file>')
        exit(-1)
    cids_with_prov = 0
    cids_without_prov = 0
    distinct_prov = set()
    distinct_relayed_prov = set()
    distinct_prov_with_addr = set()
    distinct_prov_without_addr = set()
    distinct_prov_with_location = set()
    distinct_prov_without_location = set()
    addr_type = {}
    dns_peers = {}
    processes = []
    with Pool(os.cpu_count()) as pool:
        for f in glob.glob(sys.argv[1]):
            processes.append(pool.apply_async(get_stats, (f,)))

        while processes:
            p = processes.pop(0)
            p.wait()
            dns, cwp, cwop, dp, drp, dpwa, dpwoa, dpwl, dpwol, at = p.get()
            cids_with_prov += cwp
            cids_without_prov += cwop
            distinct_prov = distinct_prov.union(dp)
            distinct_prov_with_addr = distinct_prov_with_addr.union(dpwa)
            distinct_prov_without_addr = distinct_prov_without_addr.union(dpwoa).difference(distinct_prov_with_addr)
            distinct_prov_with_location = distinct_prov_with_location.union(dpwl)
            distinct_relayed_prov = distinct_relayed_prov.union(drp).difference(distinct_prov_with_location)
            distinct_prov_without_location = distinct_prov_without_location.union(dpwol).difference(
                distinct_prov_with_location).difference(distinct_relayed_prov)

            for a, c in at.items():
                if a not in addr_type:
                    addr_type[a] = 0
                addr_type[a] += c
            for pid, addrs in dns.items():
                if pid not in dns_peers:
                    dns_peers[pid] = addrs
                else:
                    dns_peers[pid] = dns_peers[pid].union(addrs)

    with open(sys.argv[2], 'w') as w, open(f'providers_dns_names.{os.path.basename(sys.argv[2])}', 'w') as dns:
        w.write(f'cids_with_provs: {cids_with_prov}\n')
        w.write(f'cids_without_provs: {cids_without_prov}\n')
        w.write(f'distinct_provs: {len(distinct_prov)}\n')
        w.write(f'distinct_relayed_provs: {len(distinct_relayed_prov)}\n')
        w.write(f'distinct_provs_with_addr: {len(distinct_prov_with_addr)}\n')
        w.write(f'distinct_provs_without_addr: {len(distinct_prov_without_addr)}\n')
        w.write(f'distinct_provs_with_location: {len(distinct_prov_with_location)}\n')
        w.write(f'distinct_provs_without_location: {len(distinct_prov_without_location)}\n')
        for addr, count in addr_type.items():
            w.write(f'{addr}: {count}\n')

        for pid, addrs in dns_peers.items():
            dns.write(f'{pid} {addrs}\n')
