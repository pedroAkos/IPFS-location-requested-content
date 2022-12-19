import gzip
import glob
import os
import sys

from multiprocessing import Pool

import pandas as pd
from tqdm import tqdm

from scripts.parsing import gateway


# Extracts statistical information from the logs of the gateway requests.

def get_stats(file):
    good = 0
    format_errors = 0
    op_error = 0
    not_found_error = 0
    status_error = 0
    cidv1 = 0
    cidv2 = 0
    no_cid = 0
    local_req = 0
    with gzip.open(file, 'rt') as f:
        for line in tqdm(f, desc=f'Processing file {file}'):
            try:
                entry = gateway.parse_log_entry(line)
                if entry['op'] != 'GET':
                    op_error += 1
                elif 500 > int(entry['status']) >= 400:
                    not_found_error += 1
                elif 200 > int(entry['status']) >= 500:
                    status_error += 1
                elif entry['ip'] == '127.0.0.1' or entry['ip'] == '::1':
                    local_req += 1
                else:
                    cid = gateway.extract_cid(entry['http_host'], entry['target'])
                    if cid is pd.NA:
                        no_cid += 1
                    elif cid.startswith('Qm'):
                        good += 1
                        cidv1 += 1
                    else:
                        good += 1
                        cidv2 += 1

            except Exception as e:
                format_errors += 1
    return good, format_errors, op_error, not_found_error, status_error, cidv1, cidv2, no_cid, local_req


if __name__ == '__main__':
    good = 0
    format_errors = 0
    op_error = 0
    not_found_error = 0
    status_error = 0
    cidv1 = 0
    cidv2 = 0
    no_cid = 0
    local_req = 0
    processes = []
    with Pool(os.cpu_count()) as pool:
        for f in glob.glob(sys.argv[1]):
            processes.append(pool.apply_async(get_stats, (f,)))

        while processes:
            p = processes.pop(0)
            p.wait()
            g, fe, oe, nfe, se, c1, c2, nc, lr = p.get()
            good += g
            format_errors += fe
            op_error += oe
            not_found_error += nfe
            status_error += se
            cidv1 += c1
            cidv2 += c2
            no_cid += nc
            local_req += lr
    with open('stats_out.txt', 'w') as w:
        w.write(f'good: {good}\n')
        w.write(f'format_errors: {format_errors}\n')
        w.write(f'op_errors: {op_error}\n')
        w.write(f'not_found_error: {not_found_error}\n')
        w.write(f'status_error: {status_error}\n')
        w.write(f'cidv1: {cidv1}\n')
        w.write(f'cidv2: {cidv2}\n')
        w.write(f'no_cid: {no_cid}\n')
        w.write(f'local_req: {local_req}\n')
